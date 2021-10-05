#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>

#define MIN_WORD_SIZE 6

// You may find this Useful
char * delim = "\"\'.“”‘’?:;-,—*($%)! \t\n\x0A\r";

// Declare the mutex lock
pthread_mutex_t lock;

// This struct holds useful data to be passed to the threads
typedef struct ThreadWork {
    char* input;
    int listSize;
    int** mainVal;
    char*** mainKey;
    int* mainLength;

} ThreadWork;

// This array takes a new string and either adds it to the parallel arrays or it increments its value
void parallelArrayPut(int* val, char** key, int* length, char* s) {
    bool found = false;

    // Loop through the array and check if the element exists
    for(int i = 0; i < *length; i++){
        // strcasecmp is case insensitive to account for case differences
        // between the key and the token
        if(strcasecmp(key[i], s) == 0) {
            // If the token matches a key, then increment the respective value
            found = true;
            val[i] += 1;
            break;
        }
    }

    // If the loop didn't find the element, then add it to the list, give it
    // a starting value of 1, and increment the length to keep track
    if(found == false) {
        key[*length] = s;
        val[*length] = 1;
        (*length)++;
    }
    return;
}

// This function is similar to parallelArrayPut(), but it adds 2 parallel arrays together
// and merges duplicate keys
void parallelArrayAdd(ThreadWork* mainArrays, int* val, char** key, int* length) {

    // Deallocate pointers to make it easier to manage
    ThreadWork mainArray = *mainArrays;
    char** mainKey = *mainArray.mainKey;
    int* mainVal = *mainArray.mainVal;
    int currentLength = *mainArray.mainLength;

    // Loop through the main arrays trying each index of the thread's array
    for(int j = 0; j < *length; j++) {
        bool found = false;

        // Loop through the thread's array and check if the element exists in the main array
        for(int i = 0; i < currentLength; i++){
            // strcasecmp is case insensitive to account for case differences
            // between the key and the token
            if(strcasecmp(mainKey[i], key[j]) == 0) {
                // If the token matches a key, then add together
                found = true;
                mainVal[i] += val[j];
                break;
            }
        }

        // If the loop didn't find the element, then add it to the list, and give it
        // the value in the local arrays
        if(found == false) {
            mainKey[*mainArray.mainLength] = key[j];
            mainVal[*mainArray.mainLength] = val[j];
            (*mainArray.mainLength)++;
        }
    }
    return;
}

void* counterThread(void* args) {
    // Find words with more than 6 characters

    // Get data from the struct argument
    ThreadWork* data = (ThreadWork*)args;
    char* buffer = data->input;
    int listSize = data->listSize;

    // Initialize the local parallel arrays to track words and their frequencies
    int* val = malloc(sizeof(int) * listSize);
    char** key = malloc(sizeof(char*) * listSize);
    int length = 0;

    // Declare local save pointer for strtok_r and temporary token
    char* saveptr;
    char* token;

    // Use strtok_r() in a do/while loop to find tokens using the delimeter string
    token = strtok_r(buffer, delim, &saveptr);
    do
    {
        // First, check if the token is more than 6 characters long
        if(strlen(token) >= (size_t)MIN_WORD_SIZE) {
            parallelArrayPut(val, key, &length, token);
        }
        // Next, run strtok_r again to find the next token
    }
    while (token = strtok_r(NULL, delim, &saveptr));


    // Lock the Critical Section so that no other threads access the main array at the same time
    pthread_mutex_lock(&lock);

    parallelArrayAdd(data, val, key, &length);

    pthread_mutex_unlock(&lock);

    // We can free the local arrays, but the pointers that the key array holds are part of the
    // main file input to be freed at the end of the program
    free(key);
    key = NULL;

    free(val);
    val = NULL;
}

// A C quicksort implementation that sorts both parallel arrays using the value of
// the values array
void quicksort(int* value, char** key, int first,int last){
   int i;
   int j;
   int pivot;
   int temp;
   char* tempString;

   if(first < last){
      pivot = first;
      i = first;
      j = last;

      while(i < j){
         while(value[i] >= value[pivot] && i < last) {
            i++;
         }
         while(value[j] < value[pivot]) {
            j--;
         }
         if(i < j){
            temp = value[i];
            tempString = key[i];

            value[i] = value[j];
            key[i] = key[j];

            value[j] = temp;
            key[j] = tempString;
         }
      }

      temp = value[pivot];
      tempString = key[pivot];

      value[pivot] = value[j];
      key[pivot] = key[j];

      value[j] = temp;
      key[j] = tempString;

      quicksort(value, key, first,j-1);
      quicksort(value, key, j+1,last);

   }
}

int main (int argc, char *argv[])
    {
    //***TO DO***  Look at arguments, open file, divide by threads
    //             Allocate and Initialize and storage structures

    // Initialize the mutex lock
    pthread_mutex_init(&lock, NULL);

    // Find the fileName and the ThreadCount from the command line arguments
    char* FileName = argv[1];
    int ThreadCount = atoi(argv[2]);

    // Initialize thread array with the specified amount of threads
    pthread_t threads[ThreadCount];

    printf("\n\nWord Frequency Count on %s with %d threads\n", FileName, ThreadCount);

    // Create new file descriptor from opening the file
    int fd = open(FileName, O_RDONLY);

    // Check file size
    int filesize = (int)lseek(fd, (size_t)0, SEEK_END);

    // Divide the filesize by ThreadCount to find the size of the buffer sent to each thread
    int bufferSize = filesize / ThreadCount;

    // Here, I assume that the list of words 6 chars or longer will not exceed a tenth of the file length.
    int mainListSize = filesize / 10;

    // Initialize the local parallel arrays to track words and their frequencies
    int* mainVal = malloc(sizeof(int) * mainListSize);
    char** mainKey = malloc(sizeof(char*) * mainListSize);
    int length = 0;

    //This array holds the struct data for each thread, it will be freed by the thread
    ThreadWork* tw = malloc(sizeof(ThreadWork) * ThreadCount);

    // Input File is stored on the heap
    char* inputFile = malloc(sizeof(char) * filesize);

    // Read the file into inputFile and end it with a null terminator
    if(pread(fd, inputFile, filesize, 0) == -1) {
        perror("File unable to be read.");
    }

    inputFile[filesize] = '\0';

    // Allocate pointers to a chunk of memory for each thread
    char** buffer = malloc(sizeof(char*)*(ThreadCount));

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    //**************************************************************
    // *** TO DO ***  start your thread processing
    //                wait for the threads to finish
   
    // Create the data struct that will be passed to the threads, and create the threads
    int i;
    for(i = 0; i < ThreadCount; i++) {

        // Allocate memory for a chunk of the total file size and copy from the input
        buffer[i] = malloc(sizeof(char)*(bufferSize));
        strncpy(buffer[i], &inputFile[i*bufferSize], bufferSize);

        // Initialize the struct with the pointers that the thread will need
        tw[i].listSize = mainListSize;
        tw[i].input = buffer[i];
        tw[i].mainVal = &mainVal;
        tw[i].mainKey = &mainKey;
        tw[i].mainLength = &length;

        // Create the thread and pass the struct as the argument
        if(pthread_create(&threads[i], NULL, &counterThread, &tw[i]) != 0){
            perror("Thread creation failed");
        }
    }

    // Join the threads when they are done
    for(i = 0; i < ThreadCount; i++) {
        if(pthread_join(threads[i], NULL) != 0) {
            perror("Thread join failed");
        }
    }

    // Quicksort the values of both arrays and leave them in descending order
    quicksort(mainVal, mainKey, 0, length-1);

    // Print out the values to the terminal
    printf("Printing top 10 words 6 characters or more.\n");

    for(int i = 0; i < 10; i++){
        printf("Number %d is %s with a count of %d\n", i+1, mainKey[i], mainVal[i]);
    }

    // ***TO DO *** Process TOP 10 and display

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
        {
        --sec;
        n_sec = n_sec + 1000000000L;
        }

    printf("Total Time was %ld.%09ld seconds\n", sec, n_sec);
    //**************************************************************


    // ***TO DO *** cleanup

    // Use this value as the iterator for all freeing
    int f;

    // Free the main parallel arrays used.
    free(mainVal);
    mainVal = NULL;

    free(mainKey);
    mainKey = NULL;

    // Free the string used as the input buffer
    free(inputFile);
    inputFile = NULL;

    // Free the blocks of memory used by each thread as input
    for(f = 0; f < ThreadCount; f++) {
        free(buffer[f]);
        buffer[f] = NULL;
    }
    free(buffer);
    buffer = NULL;

    // Free the data structs
    free(tw);
    tw = NULL;

    // Destroy the mutex lock
    pthread_mutex_destroy(&lock);

    // Close the file since we are finished
    if(close(fd) == -1){
        perror("error closing file\n");
    }

    return 0;
    }
