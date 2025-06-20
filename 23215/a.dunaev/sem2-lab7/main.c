#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <pthread.h>

#define BUF_SIZE 8192
#define MAX_RETRY 5
#define MAX_THREADS 8
#define MAX_BUF_SIZE (1024 * 1024)

pthread_mutex_t fd_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t file_op_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char *src_entry;
    char *dst_entry;
} CopyArgs;

void *copy_thread(void *arg);

unsigned long get_tid() {
    return (unsigned long)pthread_self();
}


long get_open_fds() {
    long count = 0;
    int max_fd = getdtablesize();
    for (int fd = 0; fd < max_fd; fd++) {
        int flags = fcntl(fd, F_GETFD);
        if (flags != -1) {
            count++;
        } else if (errno != EBADF) {
            fprintf(stderr, "[TID %lu] get_open_fds: fcntl fd %d failed: %s\n", get_tid(), fd, strerror(errno));
        }
    }
    fprintf(stderr, "[TID %lu] get_open_fds: %ld open descriptors\n", get_tid(), count);
    return count;
}

long get_fd_limit() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == -1) {
        fprintf(stderr, "[TID %lu] get_fd_limit: getrlimit failed: %s\n", get_tid(), strerror(errno));
        return -1;
    }
    fprintf(stderr, "[TID %lu] get_fd_limit: limit is %ld\n", get_tid(), (long)rl.rlim_cur);
    return rl.rlim_cur;
}

FILE *safe_fopen(const char *path, const char *mode) {
    FILE *f;
    int ret = 0;
    long fd_limit = get_fd_limit();
    fprintf(stderr, "[TID %lu] safe_fopen: attempting to open %s (mode %s)\n", get_tid(), path, mode);
    while (ret < MAX_RETRY) {
        long open_fds = get_open_fds();
        if (fd_limit != -1 && open_fds != -1 && open_fds >= fd_limit - 1) {
            fprintf(stderr, "[TID %lu] safe_fopen: too many open fds (%ld/%ld), retry %d\n", 
                    get_tid(), open_fds, fd_limit, ret);
            if (ret == MAX_RETRY - 1) {
                fprintf(stderr, "[TID %lu] safe_fopen: max retries reached, failing\n", get_tid());
                return NULL;
            }
            sleep(1);
            ret++;
            continue;
        }
        f = fopen(path, mode);
        if (f) {
            fprintf(stderr, "[TID %lu] safe_fopen: opened %s successfully\n", get_tid(), path);
            return f;
        }
        if (errno == EMFILE || errno == EBADF) {
            fprintf(stderr, "[TID %lu] safe_fopen: %s on %s, retry %d\n", get_tid(), 
                    strerror(errno), path, ret);
            sleep(1);
            ret++;
        } else if (errno == EEXIST) {
            fprintf(stderr, "[TID %lu] safe_fopen: EEXIST on %s, skipping\n", get_tid(), path);
            return NULL;
        } else {
            fprintf(stderr, "[TID %lu] safe_fopen: error on %s: %s\n", get_tid(), path, strerror(errno));
            return NULL;
        }
    }
    fprintf(stderr, "[TID %lu] safe_fopen: max retries reached for %s\n", get_tid(), path);
    return NULL;
}

DIR *safe_opendir(const char *path) {
    DIR *d;
    int ret = 0;
    long fd_limit = get_fd_limit();
    fprintf(stderr, "[TID %lu] safe_opendir: attempting to open %s\n", get_tid(), path);
    while (ret < MAX_RETRY) {
        long open_fds = get_open_fds();
        if (fd_limit != -1 && open_fds != -1 && open_fds >= fd_limit - 1) {
            fprintf(stderr, "[TID %lu] safe_opendir: too many open fds (%ld/%ld), retry %d\n", 
                    get_tid(), open_fds, fd_limit, ret);
            if (ret == MAX_RETRY - 1) {
                fprintf(stderr, "[TID %lu] safe_opendir: max retries reached, failing\n", get_tid());
                return NULL;
            }
            sleep(1);
            ret++;
            continue;
        }
        d = opendir(path);
        if (d) {
            fprintf(stderr, "[TID %lu] safe_opendir: opened %s successfully\n", get_tid(), path);
            return d;
        }
        if (errno == EMFILE || errno == EBADF) {
            fprintf(stderr, "[TID %lu] safe_opendir: %s on %s, retry %d\n", get_tid(), 
                    strerror(errno), path, ret);
            sleep(1);
            ret++;
        } else if (errno == EEXIST) {
            fprintf(stderr, "[TID %lu] safe_opendir: EEXIST on %s, skipping\n", get_tid(), path);
            return NULL;
        } else {
            fprintf(stderr, "[TID %lu] safe_opendir: error on %s: %s\n", get_tid(), path, strerror(errno));
            return NULL;
        }
    }
    fprintf(stderr, "[TID %lu] safe_opendir: max retries reached for %s\n", get_tid(), path);
    return NULL;
}

int safe_fclose(FILE *f) {
    if (!f) {
        fprintf(stderr, "[TID %lu] safe_fclose: NULL file pointer\n", get_tid());
        return -1;
    }
    int r;
    int ret = 0;
    fprintf(stderr, "[TID %lu] safe_fclose: attempting to close file\n", get_tid());
    while ((r = fclose(f)) != 0 && ret < MAX_RETRY) {
        if (errno == EMFILE || errno == EBADF) {
            fprintf(stderr, "[TID %lu] safe_fclose: %s, retry %d\n", get_tid(), strerror(errno), ret);
            sleep(1);
            ret++;
            continue;
        } else {
            fprintf(stderr, "[TID %lu] safe_fclose: error: %s\n", get_tid(), strerror(errno));
            break;
        }
    }
    fprintf(stderr, "[TID %lu] safe_fclose: closed file (result: %d)\n", get_tid(), r);
    return r;
}

int safe_closedir(DIR *d) {
    int r;
    int ret = 0;
    fprintf(stderr, "[TID %lu] safe_closedir: attempting to close directory\n", get_tid());
    while ((r = closedir(d)) != 0 && ret < MAX_RETRY) {
        if (errno == EMFILE || errno == EBADF) {
            fprintf(stderr, "[TID %lu] safe_closedir: %s, retry %d\n", get_tid(), strerror(errno), ret);
            sleep(1);
            ret++;
            continue;
        } else {
            fprintf(stderr, "[TID %lu] safe_closedir: error: %s\n", get_tid(), strerror(errno));
            break;
        }
    }
    fprintf(stderr, "[TID %lu] safe_closedir: closed directory (result: %d)\n", get_tid(), r);
    return r;
}

int copy_file(const char *src_path, const char *dst_path) {
    fprintf(stderr, "[TID %lu] copy_file: copying %s to %s\n", get_tid(), src_path, dst_path);

    if (BUF_SIZE <= 0 || BUF_SIZE > MAX_BUF_SIZE) {
        fprintf(stderr, "[TID %lu] copy_file: invalid buffer size %d (must be 1 to %d)\n", 
                get_tid(), BUF_SIZE, MAX_BUF_SIZE);
        return -1;
    }

    pthread_mutex_lock(&fd_mutex);
    if (get_open_fds() >= get_fd_limit()) {
        fprintf(stderr, "[TID %lu] copy_file: too many open fds\n", get_tid());
        pthread_mutex_unlock(&fd_mutex);
        return -1;
    }
    pthread_mutex_unlock(&fd_mutex);

    pthread_mutex_lock(&file_op_mutex);
    FILE *src = safe_fopen(src_path, "rb");
    if (!src) {
        pthread_mutex_unlock(&file_op_mutex);
        return -1;
    }

    FILE *dst = safe_fopen(dst_path, "wb");
    if (!dst) {
        safe_fclose(src);
        pthread_mutex_unlock(&file_op_mutex);
        return -1;
    }

    char *buf = malloc(BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[TID %lu] copy_file: malloc failed for buffer size %d: %s\n", 
                get_tid(), BUF_SIZE, strerror(errno));
        safe_fclose(src);
        safe_fclose(dst);
        pthread_mutex_unlock(&file_op_mutex);
        return -1;
    }

    size_t n;
    while ((n = fread(buf, 1, BUF_SIZE, src)) > 0) {
        if (fwrite(buf, 1, n, dst) != n) {
            fprintf(stderr, "[TID %lu] copy_file: fwrite error: %s\n", get_tid(), strerror(errno));
            free(buf);
            safe_fclose(src);
            safe_fclose(dst);
            pthread_mutex_unlock(&file_op_mutex);
            return -1;
        }
    }
    if (ferror(src)) {
        fprintf(stderr, "[TID %lu] copy_file: fread error: %s\n", get_tid(), strerror(errno));
        free(buf);
        safe_fclose(src);
        safe_fclose(dst);
        pthread_mutex_unlock(&file_op_mutex);
        return -1;
    }

    free(buf);
    safe_fclose(src);
    safe_fclose(dst);
    pthread_mutex_unlock(&file_op_mutex);
    return 0;
}

int copy_dir(const char *src_path, const char *dst_path) {
    fprintf(stderr, "[TID %lu] copy_dir: copying %s to %s\n", get_tid(), src_path, dst_path);
    pthread_mutex_lock(&fd_mutex);
    DIR *dir = safe_opendir(src_path);
    if (!dir) {
        pthread_mutex_unlock(&fd_mutex);
        return -1;
    }
    pthread_mutex_unlock(&fd_mutex);

    char *dst_parent = strdup(dst_path);
    char *last_slash = strrchr(dst_parent, '/');
    if (last_slash) *last_slash = '\0';
    if (access(dst_parent, W_OK) == -1 && errno != ENOENT) {
        fprintf(stderr, "[TID %lu] copy_dir: parent %s not writable: %s\n", get_tid(), dst_parent, strerror(errno));
        free(dst_parent);
        safe_closedir(dir);
        return -1;
    }
    free(dst_parent);

    int ret = 0;
    while (ret < MAX_RETRY) {
        if (mkdir(dst_path, 0755) == -1) {
            if (errno == EEXIST) {
                break;
            } else if (errno == EBADF) {
                fprintf(stderr, "[TID %lu] copy_dir: mkdir %s: EBADF, retry %d\n", get_tid(), dst_path, ret);
                sleep(1);
                ret++;
                continue;
            } else {
                fprintf(stderr, "[TID %lu] copy_dir: mkdir %s: %s\n", get_tid(), dst_path, strerror(errno));
                safe_closedir(dir);
                return -1;
            }
        }
        break;
    }
    if (ret == MAX_RETRY) {
        fprintf(stderr, "[TID %lu] copy_dir: mkdir %s: max retries reached\n", get_tid(), dst_path);
        safe_closedir(dir);
        return -1;
    }

    safe_closedir(dir);

    char **entries = NULL;
    size_t entry_count = 0, entry_capacity = 10;
    entries = malloc(entry_capacity * sizeof(char *));
    if (!entries) {
        return -1;
    }

    dir = safe_opendir(src_path);
    if (!dir) {
        free(entries);
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
        if (entry_count >= entry_capacity) {
            entry_capacity *= 2;
            char **new_entries = realloc(entries, entry_capacity * sizeof(char *));
            if (!new_entries) {
                for (size_t i = 0; i < entry_count; i++) free(entries[i]);
                free(entries);
                safe_closedir(dir);
                return -1;
            }
            entries = new_entries;
        }
        entries[entry_count] = strdup(entry->d_name);
        if (!entries[entry_count]) {
            for (size_t i = 0; i < entry_count; i++) free(entries[i]);
            free(entries);
            safe_closedir(dir);
            return -1;
        }
        entry_count++;
    }
    safe_closedir(dir);

    pthread_t threads[MAX_THREADS];
    size_t active_threads = 0, entry_index = 0;
    int ret_val = 0;
    long fd_limit = get_fd_limit();

    while (entry_index < entry_count || active_threads > 0) {
        pthread_mutex_lock(&fd_mutex);
        while (entry_index < entry_count && active_threads < MAX_THREADS && 
               (fd_limit == -1 || get_open_fds() < fd_limit - 1)) {
            fprintf(stderr, "[TID %lu] copy_dir: creating thread for %s\n", get_tid(), entries[entry_index]);
            CopyArgs *args = malloc(sizeof(CopyArgs));
            if (!args) {
                fprintf(stderr, "[TID %lu] copy_dir: malloc: %s\n", get_tid(), strerror(errno));
                ret_val = -1;
                pthread_mutex_unlock(&fd_mutex);
                break;
            }

            args->src_entry = malloc(strlen(src_path) + strlen(entries[entry_index]) + 2);
            args->dst_entry = malloc(strlen(dst_path) + strlen(entries[entry_index]) + 2);
            if (!args->src_entry || !args->dst_entry) {
                fprintf(stderr, "[TID %lu] copy_dir: malloc: %s\n", get_tid(), strerror(errno));
                free(args->src_entry);
                free(args->dst_entry);
                free(args);
                ret_val = -1;
                pthread_mutex_unlock(&fd_mutex);
                break;
            }
            sprintf(args->src_entry, "%s/%s", src_path, entries[entry_index]);
            sprintf(args->dst_entry, "%s/%s", dst_path, entries[entry_index]);

            if (pthread_create(&threads[active_threads], NULL, copy_thread, args) != 0) {
                fprintf(stderr, "[TID %lu] copy_dir: pthread_create: %s\n", get_tid(), strerror(errno));
                free(args->src_entry);
                free(args->dst_entry);
                free(args);
                ret_val = -1;
                pthread_mutex_unlock(&fd_mutex);
                break;
            }
            active_threads++;
            entry_index++;
        }
        pthread_mutex_unlock(&fd_mutex);

        if (active_threads > 0) {
            fprintf(stderr, "[TID %lu] copy_dir: waiting for thread %zu\n", get_tid(), active_threads - 1);
            if (pthread_join(threads[active_threads - 1], NULL) != 0) {
                fprintf(stderr, "[TID %lu] copy_dir: pthread_join: %s\n", get_tid(), strerror(errno));
                ret_val = -1;
            }
            active_threads--;
        }
    }

    for (size_t i = 0; i < entry_count; i++) free(entries[i]);
    free(entries);
    fprintf(stderr, "[TID %lu] copy_dir: completed copying %s to %s\n", get_tid(), src_path, dst_path);
    return ret_val;
}

void *copy_thread(void *arg) {
    CopyArgs *args = (CopyArgs *)arg;
    fprintf(stderr, "[TID %lu] copy_thread: processing %s to %s\n", 
            get_tid(), args->src_entry, args->dst_entry);
    struct stat st;

    if (lstat(args->src_entry, &st) == -1) {
        fprintf(stderr, "[TID %lu] copy_thread: lstat %s: %s\n", 
                get_tid(), args->src_entry, strerror(errno));
        free(args->src_entry);
        free(args->dst_entry);
        free(args);
        return NULL;
    }

    if (S_ISDIR(st.st_mode)) {
        if (copy_dir(args->src_entry, args->dst_entry) != 0) {
            fprintf(stderr, "[TID %lu] copy_thread: failed to copy dir %s\n", 
                    get_tid(), args->src_entry);
        }
    } else if (S_ISREG(st.st_mode)) {
        if (copy_file(args->src_entry, args->dst_entry) != 0) {
            fprintf(stderr, "[TID %lu] copy_thread: failed to copy file %s\n", 
                    get_tid(), args->src_entry);
        }
    }

    free(args->src_entry);
    free(args->dst_entry);
    free(args);
    fprintf(stderr, "[TID %lu] copy_thread: finished processing %s\n", 
            get_tid(), args->src_entry);
    return NULL;
}

int main(int argc, char *argv[]) {
    fprintf(stderr, "[TID %lu] main: starting with args %s %s\n", get_tid(), argv[1], argv[2]);
    if (argc != 3) {
        fprintf(stderr, "[TID %lu] main: usage: %s SRC DST\n", get_tid(), argv[0]);
        return 1;
    }

    struct stat st;
    if (stat(argv[1], &st) < 0) {
        fprintf(stderr, "[TID %lu] main: stat %s: %s\n", get_tid(), argv[1], strerror(errno));
        return 1;
    }

    char *src_real = realpath(argv[1], NULL);
    if (!src_real) {
        fprintf(stderr, "[TID %lu] main: realpath %s: %s\n", get_tid(), argv[1], strerror(errno));
        return 1;
    }
    fprintf(stderr, "[TID %lu] main: resolved source to %s\n", get_tid(), src_real);

    struct stat dst_st;
    int dst_exists = stat(argv[2], &dst_st);
    int dst_is_dir = dst_exists == 0 && S_ISDIR(dst_st.st_mode);

    char *dst_real;
    if (dst_exists < 0 && errno == ENOENT) {
        if (S_ISREG(st.st_mode)) {
            dst_real = strdup(argv[2]);
            if (!dst_real) {
                fprintf(stderr, "[TID %lu] main: strdup: %s\n", get_tid(), strerror(errno));
                free(src_real);
                return 1;
            }
        } else {
            if (mkdir(argv[2], 0755) == -1) {
                fprintf(stderr, "[TID %lu] main: mkdir %s: %s\n", 
                        get_tid(), argv[2], strerror(errno));
                free(src_real);
                return 1;
            }
            dst_real = realpath(argv[2], NULL);
            if (!dst_real) {
                fprintf(stderr, "[TID %lu] main: realpath %s: %s\n", 
                        get_tid(), argv[2], strerror(errno));
                free(src_real);
                return 1;
            }
        }
    } else if (dst_is_dir && S_ISREG(st.st_mode)) {
        const char *src_basename = strrchr(argv[1], '/');
        src_basename = src_basename ? src_basename + 1 : argv[1];
        size_t basename_len = strlen(src_basename);
        size_t dst_len = strlen(argv[2]);
        dst_real = malloc(dst_len + basename_len + 2);
        if (!dst_real) {
            fprintf(stderr, "[TID %lu] main: strdup: %s\n", get_tid(), strerror(errno));
            free(src_real);
            return 1;
        }
        sprintf(dst_real, "%s/%s", argv[2], src_basename);
    } else {
        dst_real = realpath(argv[2], NULL);
        if (!dst_real) {
            fprintf(stderr, "[TID %lu] main: realpath %s: %s\n", 
                    get_tid(), argv[2], strerror(errno));
            free(src_real);
            return 1;
        }
    }
    fprintf(stderr, "[TID %lu] main: resolved destination to %s\n", get_tid(), dst_real);

    int ret;
    if (S_ISREG(st.st_mode)) {
        ret = copy_file(src_real, dst_real);
        if (ret != 0) {
            fprintf(stderr, "[TID %lu] main: failed to copy file %s\n", get_tid(), src_real);
        }
    } else if (S_ISDIR(st.st_mode)) {
        ret = copy_dir(src_real, dst_real);
        if (ret != 0) {
            fprintf(stderr, "[TID %lu] main: failed to copy dir %s\n", get_tid(), src_real);
        }
    } else {
        fprintf(stderr, "[TID %lu] main: source %s is neither a file nor a directory\n", 
                get_tid(), argv[1]);
        ret = -1;
    }

    free(src_real);
    free(dst_real);
    fprintf(stderr, "[TID %lu] main: completed with return code %d\n", get_tid(), ret == 0 ? 0 : 1);
    printf("opened fd %ld\n", get_open_fds());

    pthread_mutex_destroy(&fd_mutex);
    pthread_mutex_destroy(&file_op_mutex);

    return ret == 0 ? 0 : 1;
}
