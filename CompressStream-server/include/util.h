
#if defined(__APPLE__) || defined(__MACOSX)
#include <OpenCL/cl.h>
#else
#include <CL/cl.h>
#endif
#include<assert.h>
#include<math.h>
#include<sys/time.h>
#include<stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>


#include<iostream>
using namespace std;


#define WARPSIZE 64
//#define WARPSIZE 32




extern bool LoadSourceFromFile(
    const char* filename,
    char* & sourceCode );


extern int initialization2(cl_device_id* devices, cl_context* context, cl_command_queue* cmdQueue, cl_program* program, char* clFileName, cl_command_queue* cmdQueue2);

extern int initialization(cl_device_id* devices, cl_context* context, cl_command_queue* cmdQueue, cl_program* program, char* clFileName);

extern char *print_cl_errstring(cl_int err) ;

#define ALLOCATE_GPU_READ(deviceBuf, HostBuf, mem_size) \
    if(errorCode == CL_SUCCESS) { \
    deviceBuf = clCreateBuffer(context,  CL_MEM_READ_ONLY|CL_MEM_COPY_HOST_PTR, mem_size, HostBuf, &errorCode); \
    clEnqueueWriteBuffer(cmdQueue, deviceBuf, CL_TRUE, 0, mem_size, HostBuf, 0, NULL, NULL); \
    if( errorCode != CL_SUCCESS ) { printf("Error: clCreateBuffer returned %d\n", errorCode); } \
    }
#define ALLOCATE_GPU_READ_cpu(deviceBuf, HostBuf, mem_size) \
    if(errorCode == CL_SUCCESS) { \
    deviceBuf = clCreateBuffer(context,  CL_MEM_READ_ONLY|CL_MEM_ALLOC_HOST_PTR|CL_MEM_COPY_HOST_PTR, mem_size, HostBuf, &errorCode); \
    clEnqueueWriteBuffer(cmdQueue, deviceBuf, CL_TRUE, 0, mem_size, HostBuf, 0, NULL, NULL); \
    if( errorCode != CL_SUCCESS ) { printf("Error: clCreateBuffer returned %d\n", errorCode); } \
    }

#define CHECKERROR {if (errorCode != CL_SUCCESS) {fprintf(stderr, "Error at line %d code %d message %s\n", __LINE__, errorCode, print_cl_errstring(errorCode)); exit(1);}}

 extern double timestamp ();


 extern void freeObjects(cl_device_id* devices, cl_context* context, cl_command_queue* cmdQueue, cl_program* program);
extern int bytesToInt(char *);

extern long bytesToLong(char *);

extern float bytesToFloat(char *);

extern long getLong(char *input, int start);

extern int getInt(char *input, int start);

extern float getFloat(char *input, int start);

extern char *getNewChar(char *input, int start, int end);