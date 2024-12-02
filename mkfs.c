#include "wfs.h"
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

int wfs_mkfs(int raid_mode, char **diskimgs, int num_disks, int inodes, int blocks);
int setup_sb(struct wfs_sb *sb, int raid_mode, int num_disks, int inodes, int blocks, off_t disk_size);

int main(int argc, char *argv[]) {
    int raid_mode = -1; // -1 indicates unset
    char *diskimgs[MAX_DISKS];
    int num_disks = 0;
    int inodes = -1;
    int blocks = -1;
    int opt;

    // Parse command-line arguments
    while ((opt = getopt(argc, argv, "r:d:i:b:")) != -1) {
        switch (opt) {
            case 'r':
                if (strcmp(optarg, "0") == 0) {
                    raid_mode = 0;
                } else if (strcmp(optarg, "1") == 0) {
                    raid_mode = 1;
                } else if (strcmp(optarg, "1v") == 0) {
                    raid_mode = 2; // RAID 1v
                } else {
                    fprintf(stderr, "Invalid RAID mode: %s\n", optarg);
                    return 1; // Usage error
                }
                break;
            case 'd':
                if (num_disks >= MAX_DISKS) {
                    fprintf(stderr, "Too many disks specified (max %d)\n", MAX_DISKS);
                    return 1; // Usage error
                }
                diskimgs[num_disks++] = optarg;
                break;
            case 'i':
                inodes = atoi(optarg);
                break;
            case 'b':
                blocks = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: ./mkfs -r <RAID MODE> -d <DISK 1> [-d <DISK 2> ...] -i <INODES> -b <BLOCKS>\n");
                return 1; // Usage error
        }
    }

    // Validate required options
    if (raid_mode == -1) {
        fprintf(stderr, "Error: No RAID mode specified.\n");
        return 1; // Usage error
    }

    if (num_disks == 0) {
        fprintf(stderr, "Error: No disks specified.\n");
        return 1; // Usage error
    }

    if (inodes == -1 || blocks == -1) {
        fprintf(stderr, "Error: Number of inodes and blocks must be specified.\n");
        return 1; // Usage error
    }

    // Ensure minimum number of disks based on RAID mode
    if ((raid_mode == 0 || raid_mode == 1 || raid_mode == 2) && num_disks < 2) {
        fprintf(stderr, "Error: RAID %d requires at least 2 disks.\n", raid_mode);
        return 1; // Usage error
    }

    // Create the filesystem
    int result = wfs_mkfs(raid_mode, diskimgs, num_disks, inodes, blocks);
    if (result != 0) {
        return -1; // Runtime error
    }

    return 0; // Success
}

int wfs_mkfs(int raid_mode, char **diskimgs, int num_disks, int inodes, int blocks) {
    int i;
    int fd;
    struct stat statb;
    struct wfs_sb sb;

    // Align inodes and blocks to multiples of 32
    inodes = ((inodes + 31) / 32) * 32;
    blocks = ((blocks + 31) / 32) * 32;

    // TODO - use smallest disk to measure if enough space is available
    // Determine disk size from the first disk image
    fd = open(diskimgs[0], O_RDWR);
    if (fd == -1) {
        perror("Error opening disk image");
        return -1; // Runtime error
    }
    if (fstat(fd, &statb) == -1) {
        perror("Error getting file stats");
        close(fd);
        return -1; // Runtime error
    }
    off_t disk_size = statb.st_size;
    close(fd);

    // Configure the superblock
    if (setup_sb(&sb, raid_mode, num_disks, inodes, blocks, disk_size) != 0) {
        fprintf(stderr, "Error setting up superblock; disk image may be too small\n");
        return -1; // Runtime error
    }

    // Initialize the root inode
    struct wfs_inode root_inode;
    memset(&root_inode, 0, sizeof(struct wfs_inode));
    root_inode.num = 0; // Inode number 0
    root_inode.mode = S_IFDIR | 0755; // Directory with permissions
    root_inode.uid = getuid();
    root_inode.gid = getgid();
    root_inode.size = 0;
    root_inode.nlinks = 2; // '.' and '..'

    time_t current_time = time(NULL);
    root_inode.atim = current_time;
    root_inode.mtim = current_time;
    root_inode.ctim = current_time;

    // Initialize each disk image
    for (i = 0; i < num_disks; i++) {
        fd = open(diskimgs[i], O_RDWR);
        if (fd == -1) {
            perror("Error opening disk image");
            return -1; // Runtime error
        }

        // Verify disk size
        if (fstat(fd, &statb) == -1) {
            perror("Error getting file stats");
            close(fd);
            return -1; // Runtime error
        }

        if (statb.st_size < sb.d_blocks_ptr + (blocks * BLOCK_SIZE)) {
            fprintf(stderr, "Error: Disk image %s is too small\n", diskimgs[i]);
            close(fd);
            return -1; // Runtime error
        }

        // Set disk number in superblock - disk specific order
        sb.own_disk_id = i;

        // Write the superblock to the disk
        lseek(fd, 0, SEEK_SET);
        if (write(fd, &sb, sizeof(struct wfs_sb)) != sizeof(struct wfs_sb)) {
            perror("Error writing superblock");
            close(fd);
            return -1; // Runtime error
        }

        // Set up inode bitmap with root inode allocated
        size_t inode_bitmap_size = sb.d_bitmap_ptr - sb.i_bitmap_ptr;
        uint8_t *inode_bitmap = calloc(1, inode_bitmap_size);
        if (inode_bitmap == NULL) {
            perror("Error allocating inode bitmap");
            close(fd);
            return -1; // Runtime error
        }
        inode_bitmap[0] = 0x01; // Allocate root inode

        // Write inode bitmap to the disk
        lseek(fd, sb.i_bitmap_ptr, SEEK_SET);
        if (write(fd, inode_bitmap, inode_bitmap_size) != inode_bitmap_size) {
            perror("Error writing inode bitmap");
            free(inode_bitmap);
            close(fd);
            return -1; // Runtime error
        }
        free(inode_bitmap);

        // Initialize data bitmap (all blocks free)
        size_t data_bitmap_size = sb.i_blocks_ptr - sb.d_bitmap_ptr;
        uint8_t *data_bitmap = calloc(1, data_bitmap_size);
        if (data_bitmap == NULL) {
            perror("Error allocating data bitmap");
            close(fd);
            return -1; // Runtime error
        }

        // Write data bitmap to the disk
        lseek(fd, sb.d_bitmap_ptr, SEEK_SET);
        if (write(fd, data_bitmap, data_bitmap_size) != data_bitmap_size) {
            perror("Error writing data bitmap");
            free(data_bitmap);
            close(fd);
            return -1; // Runtime error
        }
        free(data_bitmap);

        // Write the root inode to the disk
        uint8_t inode_buffer[BLOCK_SIZE];
        memset(inode_buffer, 0, BLOCK_SIZE);
        memcpy(inode_buffer, &root_inode, sizeof(struct wfs_inode));

        off_t root_inode_offset = sb.i_blocks_ptr + (root_inode.num * BLOCK_SIZE);
        lseek(fd, root_inode_offset, SEEK_SET);
        if (write(fd, inode_buffer, BLOCK_SIZE) != BLOCK_SIZE) {
            perror("Error writing root inode");
            close(fd);
            return -1; // Runtime error
        }

        // Close the current disk image
        close(fd);
    }

    return 0; // Success
}

int setup_sb(struct wfs_sb *sb, int raid_mode, int num_disks, int inodes, int blocks, off_t disk_size) {
    // Calculate bitmap sizes
    size_t inode_bitmap_size = inodes / 8;
    size_t data_bitmap_size = blocks / 8;

    // Determine the size allocations
    size_t inodes_size = inodes * INODE_SIZE; // Each inode occupies one block
    size_t data_blocks_size = blocks * BLOCK_SIZE;

    // Initialize superblock fields
    sb->num_inodes = inodes;
    sb->num_data_blocks = blocks;
    sb->i_bitmap_ptr = sizeof(struct wfs_sb);
    sb->d_bitmap_ptr = sb->i_bitmap_ptr + inode_bitmap_size;

    // Align inode blocks pointer
    sb->i_blocks_ptr = ((sb->d_bitmap_ptr + data_bitmap_size + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
    sb->d_blocks_ptr = sb->i_blocks_ptr + inodes_size;

    // Add RAID configuration to superblock
    sb->raid_mode = raid_mode;
    sb->num_disks = num_disks;

    // Assign unique indexes to each disk
    for (int d = 0; d < num_disks; d++) {
        sb->raid_disk_ids[d] = d; // Assigning index d to disk d
    }

    // Verify disk size sufficiency
    off_t total_size_needed = sb->d_blocks_ptr + data_blocks_size;
    if (disk_size < total_size_needed) {
        return -1; // Insufficient disk size
    }

    return 0; // Success
}