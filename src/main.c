//
//  main.c
//  Qdir
//
//  Created by Matthew Caruana Galizia on 11/11/2016.
//  Copyright © 2016 International Consortium of Investigative Journalists. All rights reserved.
//

#define _GNU_SOURCE 1

#include <stdlib.h>
#include <unistd.h>
#include <ftw.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <getopt.h>
#include <unistd.h>
#include <libgen.h>
#include <hiredis.h>

/* POSIX.1 says each process has at least 20 file descriptors.
 * Three of those belong to the standard streams.
 * Here, we use a conservative estimate of 15 available;
 * assuming we use at most two for other uses in this program,
 * we should never run into any problems.
 * Most trees are shallower than that, so it is efficient.
 * Deeper trees are traversed fine, just a bit slower.
 * (Linux allows typically hundreds to thousands of open files,
 *  so you'll probably never see any issues even if you used
 *  a much higher value, say a couple of hundred, but
 *  15 is a safe, reasonable value.)
 */
#ifndef USE_FDS
#define USE_FDS 15
#endif

static redisContext *redis_c;
static char *queue_name = "qdir";
static bool ignore_hidden = true;
static bool verbose = false;

int queue_entry(const char *filepath, const struct stat *info, const int typeflag, struct FTW *pathinfo) {
	/* const char *const filename = filepath + pathinfo->base; */
	
	if (typeflag == FTW_SL) {
		printf("WARNING Ignoring link: \"%s\".\n", filepath);
	} else if (typeflag == FTW_SLN) {
		printf("WARNING %s (dangling symlink)\n", filepath);
	} else if (typeflag == FTW_F) {
		char *filename = basename(strdup(filepath));

		if (verbose) {
			printf("Queueing %s...\n", filepath);
		}

		if (ignore_hidden && '.' == filename[0]) {
			printf("Skipping hidden file \"%s\".../\n", filepath);
		} else {
			freeReplyObject(redisCommand(redis_c,"LPUSH %s %s", queue_name, filepath));
		}
	} else if (typeflag == FTW_D || typeflag == FTW_DP) {
		char *dirname = basename(strdup(filepath));

		if (ignore_hidden && '.' == dirname[0]) {
			printf("Skipping hidden directory \"%s\".../\n", filepath);
			return FTW_SKIP_SUBTREE;
		}

		if (verbose) {
			printf("Entering directory \"%s\".../\n", filepath);
		}
	} else if (typeflag == FTW_DNR) {
		printf("ERROR %s/ (unreadable)\n", filepath);
	} else {
		printf("ERROR %s (unknown)\n", filepath);
	}
	
	return FTW_CONTINUE;
}


int queue_directory_tree(const char *const dirpath) {
	int result;
	
	/* Invalid directory path? */
	if (dirpath == NULL || *dirpath == '\0') {
		return errno = EINVAL;
	}
	
	result = nftw(dirpath, queue_entry, USE_FDS, FTW_PHYS | FTW_ACTIONRETVAL);
	if (result >= 0) {
		errno = result;
	}
	
	return errno;
}

int connect_to_redis(const char *hostname, const int port) {
	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	redis_c = redisConnectWithTimeout(hostname, port, timeout);
	
	if (redis_c == NULL || redis_c->err) {
		if (redis_c) {
			printf("ERROR Can't connect to Redis: %s\n", redis_c->errstr);
			redisFree(redis_c);
		} else {
			printf("ERROR Can't allocate redis context.\n");
		}

		return EXIT_FAILURE;
	}
	
	return 0;
}

void print_help() {
	printf("\033[1mQdir\033[0m v1.0.0\n");
	printf("Fast, recursive queueing of files from a directory tree to Redis.\n\n");
	printf("\033[1mOptions\033[0m\n\n");
	printf("-v\t\tBe verbose.\n");
	printf("-a\t\tThe Redis server's listen address. Defaults to 127.0.0.1.\n");
	printf("-p\t\tThe Redis server port. Defaults to 6379.\n");
	printf("-q\t\tThe name of the queue (list) in Redis. Defaults to \"%s\".\n", queue_name);
	printf("-i\t\tInclude hidden files and don't skip hidden directories.\n");
	printf("-h\t\tShow this screen.\n");
	printf("\n");
	printf("\033[1mAuthor\033[0m\n\n");
	printf("Matthew Caruana Galizia <mcaruana@icij.org>\n\n");
	printf("\033[1mIssues\033[0m\n\n");
	printf("https://github.com/ICIJ/qdir/issues\n\n");
}

void print_usage() {
	printf("Usage: qdir [-ahpqv] [file ...]\n");
}

int main(int argc, char *argv[]) {

	// Default values for parameters.
	int redis_port = 6379;
	char *redis_address = "127.0.0.1";
	
	// Parse our arguments.
	int option = 0;
	while ((option = getopt(argc, argv,"hiva:p:q:")) != -1) {
		switch (option) {
			case 'h' : print_help();
				exit(EXIT_SUCCESS);
			case 'v' : verbose = true;
				break;
			case 'a' : redis_address = optarg;
				break;
			case 'p' : redis_port = atoi(optarg);
				break;
			case 'q' : queue_name = optarg;
				break;
			case 'i' : ignore_hidden = false;
				break;
			default: print_usage();
				exit(EXIT_FAILURE);
		}
	}

	// Connect to Redis.
	if (connect_to_redis(redis_address, redis_port)) {
		return EXIT_FAILURE;
	}

	if (optind >= argc) {
		print_usage();
		return EXIT_FAILURE;
	}

	while (optind < argc) {
		char *path = argv[optind++];

		if (verbose) {
			printf("Scanning \"%s\"...\n", path);
		}

		if (queue_directory_tree(path)) {
			fprintf(stderr, "%s.\n", strerror(errno));
			redisFree(redis_c);
			return EXIT_FAILURE;
		}
	}

	redisFree(redis_c);
	return EXIT_SUCCESS;
}
