#define FUSE_USE_VERSION 30

#include "wfs.h"
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>      
#include <unistd.h>    
#include <sys/mman.h>  
#include <sys/stat.h> 
#include <stdlib.h>  
#include <time.h>
#include <stdint.h>
#include <stddef.h>

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define POINTERS_PER_BLOCK (BLOCK_SIZE / sizeof(int)) // 512 / 4 = 128 pointers per indirect block

// Global Variables
void *disk_images[MAX_DISKS];
size_t disk_sizes[MAX_DISKS];
int num_disks = 0;
int raid_mode = 0; // 0: RAID 0, 1: RAID 1, 2: RAID 1v
struct wfs_sb *superblock = NULL;
int num_inodes;
int num_data_blocks;

// Bitmaps
uint8_t *inode_bitmap = NULL;
uint8_t *data_bitmap = NULL;

// Function Prototypes
int wfs_init(char **disk_paths, int num_disks_input);
void wfs_destroy();
int read_inode(int inode_num, struct wfs_inode *inode);
int write_inode(struct wfs_inode *inode);
int get_data_block_offset(int block_num, int *disk_num, off_t *offset);
int read_data_block(int block_num, void *buf);
int write_data_block(int block_num, void *buf);
int allocate_inode();
int allocate_data_block();
void free_inode(int inode_num);
void free_data_block(int block_num);
int get_inode_by_path(const char *path, struct wfs_inode *inode);
void split_path(const char *path, char *parent, char *name);
int find_dentry(struct wfs_inode *dir_inode, const char *name, struct wfs_dentry *dentry);
int add_dentry(struct wfs_inode *dir_inode, const char *name, int inode_num);
int remove_dentry(struct wfs_inode *dir_inode, const char *name);
int read_indirect_block(int indirect_block_num, int *indirect_pointers);
int write_indirect_block(int indirect_block_num, int *indirect_pointers); 
int allocate_indirect_block(struct wfs_inode *inode); 
int initialize_root_directory();

// FUSE Operations
static int wfs_getattr(const char *path, struct stat *stbuf);
static int wfs_mknod(const char *path, mode_t mode, dev_t dev);
static int wfs_mkdir(const char *path, mode_t mode);
static int wfs_unlink(const char *path);
static int wfs_rmdir(const char *path);

static int wfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
static int wfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
static int wfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);

// FUSE Operations Struct
static struct fuse_operations wfs_oper = {
    .getattr = wfs_getattr,
    .mknod   = wfs_mknod,
    .mkdir   = wfs_mkdir,
    .unlink  = wfs_unlink,
    .rmdir   = wfs_rmdir,
    .read    = wfs_read,
    .write   = wfs_write,
    .readdir = wfs_readdir,
};

// Main Function
int main(int argc, char *argv[]) {
    // Ensure there are enough arguments
    if (argc < 3) { // At least one disk and mount point
        fprintf(stderr, "Usage: %s disk1 [disk2 ...] [FUSE options] mount_point\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Parse disk images
    num_disks = 0;
    int i = 1; // Start from argv[1]
    while (i < argc && argv[i][0] != '-') {
        if (num_disks >= MAX_DISKS) {
            fprintf(stderr, "Too many disks specified (max %d)\n", MAX_DISKS);
            exit(EXIT_FAILURE);
        }
        num_disks++;
        i++;
    }

    if (num_disks == 0) {
        fprintf(stderr, "Error: No disks specified.\n");
        exit(EXIT_FAILURE);
    }

    // Calculate FUSE arguments
    int fuse_argc = argc - num_disks;
    char **fuse_argv = (char **)malloc(fuse_argc * sizeof(char *));
    if (fuse_argv == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    fuse_argv[0] = argv[0]; // Program name

    for (int j = 1; j < fuse_argc; j++) {
        fuse_argv[j] = argv[num_disks + j];
    }

    // Initialize the filesystem
    if (wfs_init(argv + 1, num_disks) != 0) {
        free(fuse_argv);
        exit(EXIT_FAILURE);
    }

    // Run FUSE
    int ret = fuse_main(fuse_argc, fuse_argv, &wfs_oper, NULL);

    // Cleanup
    wfs_destroy();
    free(fuse_argv);
    return ret;
}

// Initialize Filesystem
int wfs_init(char **disk_paths, int num_disks_input) {
    int fd;
    struct stat sbuf;

    num_disks = num_disks_input;

    // Open and map the disk images
    for (int d = 0; d < num_disks; d++) {
        fd = open(disk_paths[d], O_RDWR);
        if (fd == -1) {
            perror("Error opening disk image");
            return -1;
        }

        if (fstat(fd, &sbuf) == -1) {
            perror("Error getting file stats");
            close(fd);
            return -1;
        }

        disk_sizes[d] = sbuf.st_size;
        disk_images[d] = mmap(NULL, disk_sizes[d], PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (disk_images[d] == MAP_FAILED) {
            perror("Error mapping disk image");
            close(fd);
            return -1;
        }

        close(fd);
    }

    // Read superblock from first disk
    superblock = (struct wfs_sb *)disk_images[0];

    // Verify the filesystem was created with the same number of disks
    if (superblock->num_disks != num_disks) {
        fprintf(stderr, "Error: Disk count mismatch.\n");
        return -1;
    }

    raid_mode = superblock->raid_mode;

    if (raid_mode != 0 && raid_mode != 1 && raid_mode != 2) {
        fprintf(stderr, "Error: Unknown RAID mode.\n");
        return -1;
    }

    num_inodes = superblock->num_inodes;
    num_data_blocks = superblock->num_data_blocks;

    // Load inode and data bitmaps
    inode_bitmap = (uint8_t *)disk_images[0] + superblock->i_bitmap_ptr;
    data_bitmap = (uint8_t *)disk_images[0] + superblock->d_bitmap_ptr;

    // Initialize root directory
    if (initialize_root_directory() != 0) {
        fprintf(stderr, "Error: Failed to initialize root directory.\n");
        return -1;
    }

    return 0;
}

// Destroy Filesystem
void wfs_destroy() {
    for (int d = 0; d < num_disks; d++) {
        munmap(disk_images[d], disk_sizes[d]);
    }
}

// Read Inode
int read_inode(int inode_num, struct wfs_inode *inode) {
    if (inode_num < 0 || inode_num >= num_inodes) {
        return -EINVAL;
    }
    off_t offset = superblock->i_blocks_ptr + inode_num * INODE_SIZE;
    memcpy(inode, (char *)disk_images[0] + offset, sizeof(struct wfs_inode));
    return 0;
}

// Write Inode
int write_inode(struct wfs_inode *inode) {
    if (inode->num < 0 || inode->num >= num_inodes) {
        return -EINVAL;
    }
    off_t offset = superblock->i_blocks_ptr + inode->num * INODE_SIZE;
    for (int d = 0; d < num_disks; d++) {
        memcpy((char *)disk_images[d] + offset, inode, sizeof(struct wfs_inode));
    }
    return 0;
}

// Get Data Block Offset based on RAID Mode
int get_data_block_offset(int block_num, int *disk_num, off_t *offset) {
    if (block_num < 0 || block_num >= num_data_blocks) {
        return -EINVAL;
    }

    if (raid_mode == 0) { // RAID 0
        *disk_num = block_num % num_disks;
        int blk_idx = block_num / num_disks;
        *offset = superblock->d_blocks_ptr + blk_idx * BLOCK_SIZE;
        return 0;
    } else { // RAID 1 and 1v
        *disk_num = -1; // All disks
        *offset = superblock->d_blocks_ptr + block_num * BLOCK_SIZE;
        return 0;
    }
}

// Read Data Block
int read_data_block(int block_num, void *buf) {
    int disk_num;
    off_t offset;

    if (get_data_block_offset(block_num, &disk_num, &offset) != 0) {
        return -EINVAL;
    }

    if (raid_mode == 0) { // RAID 0
        memcpy(buf, (char *)disk_images[disk_num] + offset, BLOCK_SIZE);
    } else if (raid_mode == 1) { // RAID 1
        memcpy(buf, (char *)disk_images[0] + offset, BLOCK_SIZE);
    } else if (raid_mode == 2) { // RAID 1v
        char *blocks_data[MAX_DISKS];
        for (int d = 0; d < num_disks; d++) {
            blocks_data[d] = malloc(BLOCK_SIZE);
            if (blocks_data[d] == NULL) {
                // Free previously allocated blocks
                for (int k = 0; k < d; k++) {
                    free(blocks_data[k]);
                }
                return -ENOMEM;
            }
            memcpy(blocks_data[d], (char *)disk_images[d] + offset, BLOCK_SIZE);
        }
        // Majority voting
        int majority_count = 0;
        int majority_index = 0;
        for (int i = 0; i < num_disks; i++) {
            int count = 1;
            for (int j = i + 1; j < num_disks; j++) {
                if (memcmp(blocks_data[i], blocks_data[j], BLOCK_SIZE) == 0) {
                    count++;
                }
            }
            if (count > majority_count) {
                majority_count = count;
                majority_index = i;
            }
        }
        memcpy(buf, blocks_data[majority_index], BLOCK_SIZE);
        // Free blocks
        for (int d = 0; d < num_disks; d++) {
            free(blocks_data[d]);
        }
    }
    return 0;
}

// Write Data Block
int write_data_block(int block_num, void *buf) {
    int disk_num;
    off_t offset;

    if (get_data_block_offset(block_num, &disk_num, &offset) != 0) {
        return -EINVAL;
    }

    if (raid_mode == 0) { // RAID 0
        memcpy((char *)disk_images[disk_num] + offset, buf, BLOCK_SIZE);
    } else { // RAID 1 and 1v
        for (int d = 0; d < num_disks; d++) {
            memcpy((char *)disk_images[d] + offset, buf, BLOCK_SIZE);
        }
    }
    return 0;
}

// Allocate Inode
int allocate_inode() {
    for (int i = 0; i < num_inodes; i++) {
        uint8_t byte = inode_bitmap[i / 8];
        if ((byte & (1 << (i % 8))) == 0) {
            // Mark inode as allocated
            inode_bitmap[i / 8] |= (1 << (i % 8));
            // Mirror to other disks
            for (int d = 1; d < num_disks; d++) {
                uint8_t *ib = (uint8_t *)disk_images[d] + superblock->i_bitmap_ptr;
                ib[i / 8] = inode_bitmap[i / 8];
            }
            return i;
        }
    }
    return -ENOSPC;
}

// Allocate Data Block
int allocate_data_block() {
    // Start from 1 to avoid using block 0, but go up to and including num_data_blocks
    for (int i = 1; i <= num_data_blocks; i++) {
        uint8_t byte = data_bitmap[i / 8];
        if ((byte & (1 << (i % 8))) == 0) {
            // Mark data block as allocated
            data_bitmap[i / 8] |= (1 << (i % 8));
            // Mirror to other disks
            for (int d = 1; d < num_disks; d++) {
                uint8_t *db = (uint8_t *)disk_images[d] + superblock->d_bitmap_ptr;
                db[i / 8] = data_bitmap[i / 8];
            }
            printf("Allocated data block %d\n", i);  // Debug output
            return i;
        }
    }
    return -ENOSPC;
}

// Read Indirect Block
int read_indirect_block(int indirect_block_num, int *indirect_pointers) {
    if (indirect_block_num == 0) {
        // Indirect block not allocated
        memset(indirect_pointers, 0, POINTERS_PER_BLOCK * sizeof(int));
        return 0;
    }

    return read_data_block(indirect_block_num, (void *)indirect_pointers);
}

// Write Indirect Block
int write_indirect_block(int indirect_block_num, int *indirect_pointers) {
    if (indirect_block_num == 0) {
        // Indirect block not allocated; nothing to write
        return 0;
    }

    return write_data_block(indirect_block_num, (void *)indirect_pointers);
}

// Allocate Indirect Block (Only for regular files)
int allocate_indirect_block(struct wfs_inode *inode) {
    if (inode->blocks[IND_BLOCK] != 0) {
        // Indirect block already allocated
        return inode->blocks[IND_BLOCK];
    }

    int new_block = allocate_data_block();
    if (new_block < 0) {
        return new_block; // Propagate -ENOSPC or other errors
    }

    // Initialize the indirect block with zeros
    char zero_block[BLOCK_SIZE];
    memset(zero_block, 0, BLOCK_SIZE);
    if (write_data_block(new_block, zero_block) != 0) {
        free_data_block(new_block);
        return -EIO;
    }

    inode->blocks[IND_BLOCK] = new_block;
    if (write_inode(inode) != 0) {
        free_data_block(new_block);
        inode->blocks[IND_BLOCK] = 0;
        return -EIO;
    }

    return new_block;
}

// Free Inode and Associated Blocks
void free_inode(int inode_num) {
    if (inode_num < 0 || inode_num >= num_inodes) {
        return;
    }

    struct wfs_inode inode;
    if (read_inode(inode_num, &inode) != 0) {
        // Could not read inode; cannot free
        return;
    }

    // Free direct blocks
    for (int i = 0; i < D_BLOCK; i++) {
        if (inode.blocks[i] != 0) {
            free_data_block(inode.blocks[i]);
            inode.blocks[i] = 0;
        }
    }

    // Free indirect blocks only if it's a regular file
    if (!S_ISDIR(inode.mode) && inode.blocks[IND_BLOCK] != 0) {
        int indirect_pointers[POINTERS_PER_BLOCK];
        if (read_indirect_block(inode.blocks[IND_BLOCK], indirect_pointers) == 0) {
            for (int i = 0; i < POINTERS_PER_BLOCK; i++) {
                if (indirect_pointers[i] != 0) {
                    free_data_block(indirect_pointers[i]);
                }
            }
        }
        // Free the indirect block itself
        free_data_block(inode.blocks[IND_BLOCK]);
        inode.blocks[IND_BLOCK] = 0;
    }

    // Clear inode allocation
    inode_bitmap[inode_num / 8] &= ~(1 << (inode_num % 8));
    // Mirror to other disks
    for (int d = 1; d < num_disks; d++) {
        uint8_t *ib = (uint8_t *)disk_images[d] + superblock->i_bitmap_ptr;
        ib[inode_num / 8] = inode_bitmap[inode_num / 8];
    }
}

// Free Data Block
void free_data_block(int block_num) {
    if (block_num < 0 || block_num >= num_data_blocks) {
        return;
    }
    data_bitmap[block_num / 8] &= ~(1 << (block_num % 8));
    // Mirror to other disks
    for (int d = 1; d < num_disks; d++) {
        uint8_t *db = (uint8_t *)disk_images[d] + superblock->d_bitmap_ptr;
        db[block_num / 8] = data_bitmap[block_num / 8];
    }
}

// Initialize Root Directory
int initialize_root_directory() {
    struct wfs_inode root_inode;
    int res = read_inode(0, &root_inode);
    if (res != 0) {
        fprintf(stderr, "Error: Cannot read root inode.\n");
        return -EIO;
    }

    // Check if root inode is a directory
    if (!S_ISDIR(root_inode.mode)) {
        fprintf(stderr, "Error: Root inode is not a directory.\n");
        return -EIO;
    }

    // Allocate a data block for the root directory if not already allocated
    if (root_inode.blocks[0] == 0) {
        int block_num = allocate_data_block();
        if (block_num < 0) {
            fprintf(stderr, "Error: Cannot allocate data block for root directory.\n");
            return block_num;
        }
        root_inode.blocks[0] = block_num;
        // We'll set size to BLOCK_SIZE since we're using one full block
        root_inode.size = BLOCK_SIZE;

        // Initialize block with all entries marked as available (-1)
        char block[BLOCK_SIZE];
        memset(block, 0, BLOCK_SIZE);
        struct wfs_dentry *entries = (struct wfs_dentry *)block;
        
        // Mark all entries as available
        for (int j = 0; j < BLOCK_SIZE / sizeof(struct wfs_dentry); j++) {
            entries[j].num = -1;
        }

        // Add '.' and '..' entries at the beginning
        strncpy(entries[0].name, ".", MAX_NAME_LEN);
        entries[0].name[MAX_NAME_LEN - 1] = '\0';
        entries[0].num = 0;  // Root inode

        strncpy(entries[1].name, "..", MAX_NAME_LEN);
        entries[1].name[MAX_NAME_LEN - 1] = '\0';
        entries[1].num = 0;  // Also root inode for root directory

        if (write_data_block(block_num, block) != 0) {
            fprintf(stderr, "Error: Cannot write root directory entries.\n");
            free_data_block(block_num);
            return -EIO;
        }

        root_inode.nlinks = 2;  // '.' and '..'
        root_inode.mtim = time(NULL);
        root_inode.ctim = time(NULL);

        if (write_inode(&root_inode) != 0) {
            fprintf(stderr, "Error: Cannot update root inode.\n");
            free_data_block(block_num);
            return -EIO;
        }

        printf("Root directory initialized with size %ld bytes\n", root_inode.size);

        printf("Root directory initialized: inode 0, block %ld, size %ld\n", root_inode.blocks[0], root_inode.size);
    }

    return 0;
}

// Get Inode by Path
int get_inode_by_path(const char *path, struct wfs_inode *inode) {
    if (strcmp(path, "/") == 0) {
        // Root directory
        return read_inode(0, inode);
    }

    char *path_copy = strdup(path);
    if (path_copy == NULL) {
        return -ENOMEM;
    }

    char *rest = path_copy;
    // Skip leading slashes
    while (*rest == '/') rest++;

    struct wfs_inode current_inode;
    if (read_inode(0, &current_inode) != 0) {
        free(path_copy);
        return -ENOENT;
    }

    char *token = strtok_r(rest, "/", &rest);
    while (token != NULL) {
        if (!S_ISDIR(current_inode.mode)) {
            free(path_copy);
            return -ENOTDIR;
        }

        // Search for token in current directory
        struct wfs_dentry dentry;
        int res = find_dentry(&current_inode, token, &dentry);
        if (res != 0) {
            free(path_copy);
            return res;
        }
        if (read_inode(dentry.num, &current_inode) != 0) {
            free(path_copy);
            return -EIO;
        }

        token = strtok_r(NULL, "/", &rest);
    }

    memcpy(inode, &current_inode, sizeof(struct wfs_inode));
    free(path_copy);
    return 0;
}

// Split Path into Parent and Name
void split_path(const char *path, char *parent, char *name) {
    char temp_path[MAX_PATH_LEN];
    strncpy(temp_path, path, MAX_PATH_LEN);
    temp_path[MAX_PATH_LEN - 1] = '\0';

    char *last_slash = strrchr(temp_path, '/');
    if (last_slash == temp_path) {
        // Root directory
        strcpy(parent, "/");
        strcpy(name, last_slash + 1);
    } else if (last_slash != NULL) {
        *last_slash = '\0';
        strcpy(parent, temp_path);
        strcpy(name, last_slash + 1);
    } else {
        strcpy(parent, ".");
        strcpy(name, temp_path);
    }
}

// Adjusted find_dentry function
int find_dentry(struct wfs_inode *dir_inode, const char *name, struct wfs_dentry *dentry) {
    if (!S_ISDIR(dir_inode->mode)) {
        return -ENOTDIR;
    }

    // If directory has no data blocks, it is empty
    int has_blocks = 0;
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode->blocks[i] != 0) {
            has_blocks = 1;
            break;
        }
    }

    if (!has_blocks) {
        return -ENOENT;
    }

    // Iterate over direct blocks
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode->blocks[i] == 0) {
            continue;
        }
        char block[BLOCK_SIZE];
        if (read_data_block(dir_inode->blocks[i], block) != 0) {
            return -EIO;
        }
        struct wfs_dentry *entries = (struct wfs_dentry *)block;
        for (int j = 0; j < BLOCK_SIZE / sizeof(struct wfs_dentry); j++) {
            if (entries[j].num == -1) {
                continue;
            }
            if (strncmp(entries[j].name, name, MAX_NAME_LEN) == 0) {
                if (dentry != NULL) {
                    memcpy(dentry, &entries[j], sizeof(struct wfs_dentry));
                }
                return 0;
            }
        }
    }

    return -ENOENT;
}

// Add directory entry
int add_dentry(struct wfs_inode *dir_inode, const char *name, int inode_num) {
    printf("\nAdd_dentry: Starting to add entry '%s' (inode %d)\n", name, inode_num);
    printf("Add_dentry: Parent inode number: %d\n", dir_inode->num);
    printf("Add_dentry: Parent directory blocks:\n");
    for (int i = 0; i < D_BLOCK; i++) {
        printf("  blocks[%d] = %ld\n", i, dir_inode->blocks[i]);
    }
    
    if (!S_ISDIR(dir_inode->mode)) {
        printf("Add_dentry: Error - parent is not a directory\n");
        return -ENOTDIR;
    }

    // Create the new directory entry
    struct wfs_dentry new_entry;
    memset(&new_entry, 0, sizeof(struct wfs_dentry));
    strncpy(new_entry.name, name, MAX_NAME_LEN);
    new_entry.name[MAX_NAME_LEN - 1] = '\0';
    new_entry.num = inode_num;

    printf("Add_dentry: Parent directory current size: %ld bytes\n", dir_inode->size);
    printf("Add_dentry: Checking existing blocks for free space...\n");

    printf("Add_dentry: Directory has %d direct blocks\n", D_BLOCK);

    // First, try to find space in existing blocks
    for (int i = 0; i < D_BLOCK; i++) {
        printf("Add_dentry: Checking block[%d] = %ld\n", i, dir_inode->blocks[i]);
        if (dir_inode->blocks[i] != 0) {  // If this block exists
            printf("Add_dentry: Examining block[%d] = %ld\n", i, dir_inode->blocks[i]);
            
            char block[BLOCK_SIZE];
            if (read_data_block(dir_inode->blocks[i], block) != 0) {
                printf("Add_dentry: Error reading data block %ld\n", dir_inode->blocks[i]);
                return -EIO;
            }

            struct wfs_dentry *entries = (struct wfs_dentry *)block;
            int entries_per_block = BLOCK_SIZE / sizeof(struct wfs_dentry);
            printf("Add_dentry: Block can hold %d entries\n", entries_per_block);

            // Look for a free slot
            for (int j = 0; j < entries_per_block; j++) {
                printf("Add_dentry: Checking slot %d (inode num: %d)\n", j, entries[j].num);
                if (entries[j].num == -1) {  // Found a free slot
                    printf("Add_dentry: Found free slot at index %d in block %d\n", j, i);
                    entries[j] = new_entry;
                    if (write_data_block(dir_inode->blocks[i], block) != 0) {
                        printf("Add_dentry: Error writing to data block\n");
                        return -EIO;
                    }
                    printf("Add_dentry: Successfully added entry to existing block\n");
                    return 0;  // Successfully added to existing block
                }
            }
            printf("Add_dentry: No free slots in block %d\n", i);
        }
    }

    printf("Add_dentry: No free space in existing blocks, need to allocate new block\n");

    // If we get here, we need a new block
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode->blocks[i] == 0) {
            printf("Add_dentry: Found empty block pointer at index %d\n", i);
            
            int block_num = allocate_data_block();
            if (block_num < 0) {
                printf("Add_dentry: Failed to allocate new data block\n");
                return block_num;
            }
            printf("Add_dentry: Allocated new block %d\n", block_num);

            char block[BLOCK_SIZE];
            memset(block, 0, BLOCK_SIZE);
            struct wfs_dentry *entries = (struct wfs_dentry *)block;
            
            // Initialize all entries as empty
            int entries_per_block = BLOCK_SIZE / sizeof(struct wfs_dentry);
            for (int j = 0; j < entries_per_block; j++) {
                entries[j].num = -1;
            }
            printf("Add_dentry: Initialized %d directory entries as empty\n", entries_per_block);

            // Add our new entry in the first slot
            entries[0] = new_entry;

            if (write_data_block(block_num, block) != 0) {
                printf("Add_dentry: Error writing new data block\n");
                free_data_block(block_num);
                return -EIO;
            }

            dir_inode->blocks[i] = block_num;
            dir_inode->size += BLOCK_SIZE;
            printf("Add_dentry: Updated directory size to %ld bytes\n", dir_inode->size);

            if (write_inode(dir_inode) != 0) {
                printf("Add_dentry: Error writing inode\n");
                return -EIO;
            }

            printf("Add_dentry: Successfully added entry in new block\n");
            return 0;
        }
    }

    printf("Add_dentry: Error - no space left in directory\n");
    return -ENOSPC;  // No space left
}

// Remove Directory Entry
int remove_dentry(struct wfs_inode *dir_inode, const char *name) {
    if (!S_ISDIR(dir_inode->mode)) {
        return -ENOTDIR;
    }

    int removed = 0;

    // Iterate over direct blocks only (no indirect blocks for directories)
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode->blocks[i] == 0) {
            continue;
        }
        char block[BLOCK_SIZE];
        if (read_data_block(dir_inode->blocks[i], block) != 0) {
            return -EIO;
        }
        struct wfs_dentry *entries = (struct wfs_dentry *)block;
        for (int j = 0; j < BLOCK_SIZE / sizeof(struct wfs_dentry); j++) {
            if (entries[j].num == -1) {
                continue;
            }
            if (strncmp(entries[j].name, name, MAX_NAME_LEN) == 0) {
                entries[j].num = -1; // Mark as deleted
                if (write_data_block(dir_inode->blocks[i], block) != 0) {
                    return -EIO;
                }
                removed = 1;
                break;
            }
        }
        if (removed) {
            break;
        }
    }

    if (!removed) {
        return -ENOENT;
    }

    return 0;
}

// Read File
static int wfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void) fi; // Unused

    struct wfs_inode inode;
    int res = get_inode_by_path(path, &inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISREG(inode.mode)) {
        return -EISDIR;
    }

    if (offset >= inode.size) {
        return 0;
    }

    if (offset + size > inode.size) {
        size = inode.size - offset;
    }

    size_t bytes_read = 0;
    while (size > 0) {
        int block_idx = offset / BLOCK_SIZE;
        int block_offset = offset % BLOCK_SIZE;

        off_t block_num;
        if (block_idx < D_BLOCK) {
            // Direct blocks
            block_num = inode.blocks[block_idx];
        } else {
            // Indirect blocks
            if (inode.blocks[IND_BLOCK] == 0) {
                // Indirect block not allocated; treat as zeros
                memset(buf + bytes_read, 0, MIN(BLOCK_SIZE - block_offset, size));
                size -= MIN(BLOCK_SIZE - block_offset, size);
                bytes_read += MIN(BLOCK_SIZE - block_offset, size);
                offset += MIN(BLOCK_SIZE - block_offset, size);
                continue;
            }

            // Read indirect block pointers
            int indirect_pointers[POINTERS_PER_BLOCK];
            res = read_indirect_block(inode.blocks[IND_BLOCK], indirect_pointers);
            if (res != 0) {
                return res;
            }

            int indirect_idx = block_idx - D_BLOCK;
            if (indirect_idx >= POINTERS_PER_BLOCK) {
                // Exceeds maximum file size supported
                break;
            }

            block_num = indirect_pointers[indirect_idx];
            if (block_num == 0) {
                // Data block not allocated; treat as zeros
                memset(buf + bytes_read, 0, MIN(BLOCK_SIZE - block_offset, size));
                size -= MIN(BLOCK_SIZE - block_offset, size);
                bytes_read += MIN(BLOCK_SIZE - block_offset, size);
                offset += MIN(BLOCK_SIZE - block_offset, size);
                continue;
            }
        }

        if (block_num == 0) {
            // Block not allocated; treat as zeros
            memset(buf + bytes_read, 0, MIN(BLOCK_SIZE - block_offset, size));
            size -= MIN(BLOCK_SIZE - block_offset, size);
            bytes_read += MIN(BLOCK_SIZE - block_offset, size);
            offset += MIN(BLOCK_SIZE - block_offset, size);
            continue;
        }

        char block[BLOCK_SIZE];
        res = read_data_block(block_num, block);
        if (res != 0) {
            return res;
        }

        size_t bytes_to_copy = MIN(BLOCK_SIZE - block_offset, size);
        memcpy(buf + bytes_read, block + block_offset, bytes_to_copy);
        bytes_read += bytes_to_copy;
        offset += bytes_to_copy;
        size -= bytes_to_copy;
    }

    return bytes_read;
}

// Write File
static int wfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void) fi; // Unused

    struct wfs_inode inode;
    int res = get_inode_by_path(path, &inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISREG(inode.mode)) {
        return -EISDIR;
    }

    size_t bytes_written = 0;
    while (size > 0) {
        int block_idx = offset / BLOCK_SIZE;
        int block_offset = offset % BLOCK_SIZE;

        off_t block_num;
        if (block_idx < D_BLOCK) {
            // Direct blocks
            if (inode.blocks[block_idx] == 0) {
                // Allocate a data block
                int new_block = allocate_data_block();
                if (new_block < 0) {
                    return -ENOSPC;
                }
                inode.blocks[block_idx] = new_block;
            }
            block_num = inode.blocks[block_idx];
        } else {
            // Indirect blocks
            int indirect_block_num = allocate_indirect_block(&inode);
            if (indirect_block_num < 0) {
                return indirect_block_num; // -ENOSPC or other errors
            }

            // Read indirect block pointers
            int indirect_pointers[POINTERS_PER_BLOCK];
            res = read_indirect_block(inode.blocks[IND_BLOCK], indirect_pointers);
            if (res != 0) {
                return res;
            }

            int indirect_idx = block_idx - D_BLOCK;
            if (indirect_idx >= POINTERS_PER_BLOCK) {
                // Exceeds maximum file size supported
                return -ENOSPC;
            }

            if (indirect_pointers[indirect_idx] == 0) {
                // Allocate a new data block
                int new_block = allocate_data_block();
                if (new_block < 0) {
                    return -ENOSPC;
                }
                indirect_pointers[indirect_idx] = new_block;

                // Write back the updated indirect block
                res = write_indirect_block(inode.blocks[IND_BLOCK], indirect_pointers);
                if (res != 0) {
                    free_data_block(new_block);
                    return res;
                }
            }

            block_num = indirect_pointers[indirect_idx];
        }

        char block[BLOCK_SIZE];
        if (read_data_block(block_num, block) != 0) {
            return -EIO;
        }

        size_t bytes_to_copy = MIN(BLOCK_SIZE - block_offset, size);
        memcpy(block + block_offset, buf + bytes_written, bytes_to_copy);

        if (write_data_block(block_num, block) != 0) {
            return -EIO;
        }

        bytes_written += bytes_to_copy;
        offset += bytes_to_copy;
        size -= bytes_to_copy;
    }

    // Update inode size if needed
    if (offset > inode.size) {
        inode.size = offset;
    }
    inode.mtim = time(NULL);
    res = write_inode(&inode);
    if (res != 0) {
        return res;
    }

    return bytes_written;
}

// Adjusted wfs_readdir function
static int wfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    (void) fi;

    struct wfs_inode dir_inode;
    int res = get_inode_by_path(path, &dir_inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISDIR(dir_inode.mode)) {
        return -ENOTDIR;
    }

    // Check if directory has data blocks
    int has_blocks = 0;
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode.blocks[i] != 0) {
            has_blocks = 1;
            break;
        }
    }

    if (!has_blocks) {
        // Even an empty directory should have "." and ".." entries
        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);
        return 0;
    }

    // Iterate over direct blocks
    for (int i = 0; i < D_BLOCK; i++) {
        if (dir_inode.blocks[i] != 0) {
            char block[BLOCK_SIZE];
            if (read_data_block(dir_inode.blocks[i], block) != 0) {
                return -EIO;
            }
            struct wfs_dentry *entries = (struct wfs_dentry *)block;
            for (int j = 0; j < BLOCK_SIZE / sizeof(struct wfs_dentry); j++) {
                if (entries[j].num != -1) {
                    filler(buf, entries[j].name, NULL, 0);
                }
            }
        }
    }

    return 0;
}

// Create Node (File)
static int wfs_mknod(const char *path, mode_t mode, dev_t dev) {
    char parent_path[MAX_PATH_LEN];
    char name[MAX_NAME_LEN];

    split_path(path, parent_path, name);

    struct wfs_inode parent_inode;
    int res = get_inode_by_path(parent_path, &parent_inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        return -ENOTDIR;
    }

    // Check if file already exists
    if (find_dentry(&parent_inode, name, NULL) == 0) {
        return -EEXIST;
    }

    // Allocate inode
    int inode_num = allocate_inode();
    if (inode_num < 0) {
        return inode_num;
    }

    // Create inode
    struct wfs_inode new_inode;
    memset(&new_inode, 0, sizeof(struct wfs_inode));
    new_inode.num = inode_num;
    new_inode.mode = mode;
    new_inode.uid = getuid();
    new_inode.gid = getgid();
    new_inode.size = 0;
    new_inode.nlinks = 1;
    time_t now = time(NULL);
    new_inode.atim = now;
    new_inode.mtim = now;
    new_inode.ctim = now;

    if (write_inode(&new_inode) != 0) {
        free_inode(inode_num);
        return -EIO;
    }

    // Add to parent directory
    if (add_dentry(&parent_inode, name, inode_num) != 0) {
        free_inode(inode_num);
        return -ENOSPC;
    }

    // Update parent inode
    parent_inode.mtim = now;
    write_inode(&parent_inode);

    return 0;
}

// Adjusted wfs_mkdir function
static int wfs_mkdir(const char *path, mode_t mode) {
    char parent_path[MAX_PATH_LEN];
    char name[MAX_NAME_LEN];

    split_path(path, parent_path, name);

    struct wfs_inode parent_inode;
    int res = get_inode_by_path(parent_path, &parent_inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        return -ENOTDIR;
    }

    // Check if directory already exists
    if (find_dentry(&parent_inode, name, NULL) == 0) {
        return -EEXIST;
    }

    // Allocate inode
    int inode_num = allocate_inode();
    if (inode_num < 0) {
        return inode_num;
    }

    // Create inode
    struct wfs_inode new_inode;
    memset(&new_inode, 0, sizeof(struct wfs_inode));
    new_inode.num = inode_num;
    new_inode.mode = mode | S_IFDIR;
    new_inode.uid = getuid();
    new_inode.gid = getgid();
    new_inode.nlinks = 2;  // For '.' and '..'
    time_t now = time(NULL);
    new_inode.atim = now;
    new_inode.mtim = now;
    new_inode.ctim = now;
    // No size or blocks allocated - we'll do that lazily
    new_inode.size = 0;

    if (write_inode(&new_inode) != 0) {
        free_inode(inode_num);
        return -EIO;
    }

    // Add to parent directory
    if (add_dentry(&parent_inode, name, inode_num) != 0) {
        free_inode(inode_num);
        return -ENOSPC;
    }

    // Update parent inode
    parent_inode.nlinks++;
    parent_inode.mtim = now;
    write_inode(&parent_inode);

    return 0;
}

// Unlink (Delete File)
static int wfs_unlink(const char *path) {
    char parent_path[MAX_PATH_LEN];
    char name[MAX_NAME_LEN];

    split_path(path, parent_path, name);

    struct wfs_inode parent_inode;
    int res = get_inode_by_path(parent_path, &parent_inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        return -ENOTDIR;
    }

    struct wfs_dentry dentry;
    if (find_dentry(&parent_inode, name, &dentry) != 0) {
        return -ENOENT;
    }

    struct wfs_inode inode;
    if (read_inode(dentry.num, &inode) != 0) {
        return -EIO;
    }

    if (S_ISDIR(inode.mode)) {
        return -EISDIR;
    }

    // Remove from parent directory
    if (remove_dentry(&parent_inode, name) != 0) {
        return -EIO;
    }

    // Decrease link count
    inode.nlinks--;
    if (inode.nlinks == 0) {
        // Free data blocks and indirect blocks
        free_inode(inode.num);
    } else {
        write_inode(&inode);
    }

    // Update parent inode
    parent_inode.mtim = time(NULL);
    write_inode(&parent_inode);

    return 0;
}

// Remove Directory
static int wfs_rmdir(const char *path) {
    char parent_path[MAX_PATH_LEN];
    char name[MAX_NAME_LEN];

    split_path(path, parent_path, name);

    struct wfs_inode parent_inode;
    int res = get_inode_by_path(parent_path, &parent_inode);
    if (res != 0) {
        return res;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        return -ENOTDIR;
    }

    struct wfs_dentry dentry;
    if (find_dentry(&parent_inode, name, &dentry) != 0) {
        return -ENOENT;
    }

    struct wfs_inode dir_inode;
    if (read_inode(dentry.num, &dir_inode) != 0) {
        return -EIO;
    }

    if (!S_ISDIR(dir_inode.mode)) {
        return -ENOTDIR;
    }

    // Check if directory is empty (excluding '.' and '..')
    int is_empty = 1;
    // Iterate over direct blocks
    for (int i = 0; i < D_BLOCK && is_empty; i++) {
        if (dir_inode.blocks[i] == 0) {
            continue;
        }
        char block[BLOCK_SIZE];
        if (read_data_block(dir_inode.blocks[i], block) != 0) {
            return -EIO;
        }
        struct wfs_dentry *entries = (struct wfs_dentry *)block;
        for (int j = 0; j < BLOCK_SIZE / sizeof(struct wfs_dentry); j++) {
            if (entries[j].num != -1 && strcmp(entries[j].name, ".") != 0 && strcmp(entries[j].name, "..") != 0) {
                is_empty = 0;
                break;
            }
        }
    }

    if (!is_empty) {
        return -ENOTEMPTY;
    }

    // Remove from parent directory
    if (remove_dentry(&parent_inode, name) != 0) {
        return -EIO;
    }

    // Free directory data blocks and indirect blocks
    free_inode(dir_inode.num);

    // Update parent inode
    parent_inode.nlinks--;
    parent_inode.mtim = time(NULL);
    write_inode(&parent_inode);

    return 0;
}

// Get Attribute
static int wfs_getattr(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
    struct wfs_inode inode;
    int res = get_inode_by_path(path, &inode);
    if (res != 0) {
        return res;
    }

    stbuf->st_mode = inode.mode;
    stbuf->st_nlink = inode.nlinks;
    stbuf->st_uid = inode.uid;
    stbuf->st_gid = inode.gid;
    stbuf->st_size = inode.size;
    stbuf->st_atime = inode.atim;
    stbuf->st_mtime = inode.mtim;
    stbuf->st_ctime = inode.ctim;

    return 0;
}
