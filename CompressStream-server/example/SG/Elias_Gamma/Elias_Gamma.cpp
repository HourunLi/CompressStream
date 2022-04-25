#include <iostream>
#include "Elias_Gamma.h"
#include "../../../include/util.h"
#include <cstring>
#include <stdlib.h>
#include <sys/time.h>
//#define WINDOW_NUM 150
#define WINDOW_NUM 100
#define TUPLE_NUM 1024
#define TUPLE_SIZE 18
#define G_TUPLE_NUM 1024
#define P_TUPLE_SIZE_1 12
#define P_TUPLE_SIZE_2 17
#define G_WINDOW_NUM 50689
using namespace std;

cl_mem devProjInput_a;
cl_mem devProjOutput_a;
cl_mem devAvgOutput_a;

cl_mem devProjInput_b;
cl_mem devProjOutput_b;
cl_mem devSortOutput_b;
cl_mem devAvgOutput_b;

// bool   GPU_available;

// cl_kernel kernel_gpu_projection;
// cl_kernel kernel_gpu_avg;
cl_kernel kernel_cpu_projection1;
cl_kernel kernel_cpu_avg1;
cl_kernel kernel_cpu_projection2;
cl_kernel kernel_cpu_avg2;
cl_kernel kernel_cpu_sort;


cl_int errorCode = CL_SUCCESS;
cl_device_id* devices = NULL;
cl_context context = NULL;
cl_command_queue cmdQueue_gpu = NULL;
cl_command_queue cmdQueue_cpu = NULL;
cl_program program = NULL;
char clfilename[1000];

int count1, count2 = 0;

JNIEXPORT void JNICALL Java_Main_init
(JNIEnv *env, jclass jcl){
//GPU_available = true;
    sprintf(clfilename, "%s%s", ".", "./example/SG/Elias_Gamma/kernel.cl");
    assert(initialization(devices, &context, &cmdQueue_cpu, &program, clfilename) == 1);
  
    devProjInput_a  = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*TUPLE_SIZE*TUPLE_NUM*WINDOW_NUM, NULL, &errorCode); CHECKERROR;
    devProjOutput_a  = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*P_TUPLE_SIZE_1*TUPLE_NUM*WINDOW_NUM, NULL, &errorCode); CHECKERROR;
    devAvgOutput_a = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*P_TUPLE_SIZE_1*TUPLE_NUM*G_WINDOW_NUM, NULL, &errorCode); CHECKERROR;

    devProjInput_b  = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*TUPLE_SIZE*TUPLE_NUM*WINDOW_NUM, NULL, &errorCode); CHECKERROR;
    devProjOutput_b = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*P_TUPLE_SIZE_2*TUPLE_NUM*WINDOW_NUM, NULL, &errorCode); CHECKERROR;
    devSortOutput_b = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*P_TUPLE_SIZE_2*TUPLE_NUM*WINDOW_NUM, NULL, &errorCode); CHECKERROR;
    devAvgOutput_b = clCreateBuffer(context, CL_MEM_READ_WRITE|CL_MEM_ALLOC_HOST_PTR, sizeof(char)*P_TUPLE_SIZE_2*TUPLE_NUM*G_WINDOW_NUM, NULL, &errorCode); CHECKERROR;


    // kernel_gpu_projection = clCreateKernel(program, "gpu_projection", &errorCode); CHECKERROR;
    // kernel_gpu_avg = clCreateKernel(program, "gpu_avg", &errorCode); CHECKERROR;
    kernel_cpu_projection1 = clCreateKernel(program, "cpu_projection1", &errorCode); CHECKERROR;
    kernel_cpu_avg1 = clCreateKernel(program, "cpu_avg1", &errorCode); CHECKERROR;
    kernel_cpu_projection2 = clCreateKernel(program, "cpu_projection2", &errorCode); CHECKERROR;
    kernel_cpu_avg2 = clCreateKernel(program, "cpu_avg2", &errorCode); CHECKERROR;
    kernel_cpu_sort = clCreateKernel(program, "cpu_sort", &errorCode); CHECKERROR;
	
	
}


JNIEXPORT void JNICALL Java_Main_processAStep1
  (JNIEnv *env, jclass jcl, jbyteArray jdata){
    // cout << "1" << endl;
    int dataSize = (int) env->GetArrayLength(jdata);
	jbyte *jbytedata = env->GetByteArrayElements(jdata, 0);
	char *batch = (char *) jbytedata;  
	
	clEnqueueWriteBuffer(cmdQueue_cpu, devProjInput_a , CL_TRUE, 0, sizeof(char)*TUPLE_SIZE*TUPLE_NUM*WINDOW_NUM, batch, 0, NULL, NULL);  
	if( errorCode != CL_SUCCESS ) { printf("Error: clCreateBuffer returned %d\n", errorCode); }
  // cout << devProjInput_a;
	
	//-----------------------------------------projection------------------------------------------------------
	errorCode = clSetKernelArg(kernel_cpu_projection1 , 0, sizeof(cl_mem), &devProjInput_a); CHECKERROR;
  errorCode = clSetKernelArg(kernel_cpu_projection1 , 1, sizeof(cl_mem), &devProjOutput_a); CHECKERROR;
	
	size_t globalsize_proj[] = {3};
  size_t blocksize_proj[] = {1};

	errorCode = clEnqueueNDRangeKernel(cmdQueue_cpu, kernel_cpu_projection1, 1, NULL, globalsize_proj, blocksize_proj, 0, NULL, NULL); CHECKERROR;
  clFinish(cmdQueue_cpu);

  errorCode = clSetKernelArg(kernel_cpu_avg1 , 0, sizeof(cl_mem), &devProjOutput_a); CHECKERROR;
  errorCode = clSetKernelArg(kernel_cpu_avg1 , 1, sizeof(cl_mem), &devAvgOutput_a); CHECKERROR;
	
	// size_t globalsize_avg[] = {256*50689};
 //    size_t blocksize_avg[] = {256};
    // size_t globalsize_avg[] = {16*50689};
    // size_t blocksize_avg[] = {16};
    size_t globalsize_avg[] = {16};
    size_t blocksize_avg[] = {16};
	

     // double a = timestamp();
    //while(GPU_available==false);
   // GPU_available=false;
    //clFinish(cmdQueue_cpu);
    errorCode = clEnqueueNDRangeKernel(cmdQueue_cpu, kernel_cpu_avg1, 1, NULL, globalsize_avg, blocksize_avg, 0, NULL, NULL); CHECKERROR;
    clFinish(cmdQueue_cpu);
    //GPU_available=true;

     // double b = timestamp();

//      count1 += b-a;
//      count2 += 1;
// cout<<"A gpu avg"<<count1/count2<<endl;
    env->ReleaseByteArrayElements(jdata, jbytedata, 0);

  }


JNIEXPORT void JNICALL Java_Main_processBStep1
  (JNIEnv *env, jclass jcl, jbyteArray jdata){  
  
    int dataSize = (int) env->GetArrayLength(jdata);
	jbyte *jbytedata = env->GetByteArrayElements(jdata, 0);
	char *batch = (char *) jbytedata;  
	
	clEnqueueWriteBuffer(cmdQueue_cpu, devProjInput_b , CL_TRUE, 0, sizeof(char)*TUPLE_SIZE*TUPLE_NUM*WINDOW_NUM, batch, 0, NULL, NULL);  
	if( errorCode != CL_SUCCESS ) { printf("Error: clCreateBuffer returned %d\n", errorCode); }
	
	//-----------------------------------------projection------------------------------------------------------
	errorCode = clSetKernelArg(kernel_cpu_projection2 , 0, sizeof(cl_mem), &devProjInput_b); CHECKERROR;
  errorCode = clSetKernelArg(kernel_cpu_projection2 , 1, sizeof(cl_mem), &devProjOutput_b); CHECKERROR;
	
	size_t globalsize_proj[] = {3};
  size_t blocksize_proj[] = {1};
	

      //double a = timestamp();
	errorCode = clEnqueueNDRangeKernel(cmdQueue_cpu, kernel_cpu_projection2, 1, NULL, globalsize_proj, blocksize_proj, 0, NULL, NULL); CHECKERROR;
  clFinish(cmdQueue_cpu);

      //double b = timestamp();
//cout<<"B cpu proj"<<(b-a)/1000000<<endl;
//==========================================
//
//
//
//
  errorCode = clSetKernelArg(kernel_cpu_sort , 0, sizeof(cl_mem), &devProjOutput_b); CHECKERROR;
  errorCode = clSetKernelArg(kernel_cpu_sort , 1, sizeof(cl_mem), &devSortOutput_b); CHECKERROR;

  // size_t globalsize_group[] = {G_TUPLE_NUM* G_WINDOW_NUM};
  // size_t blocksize_group[] = {G_TUPLE_NUM};
  size_t globalsize_group[] = {16};
  size_t blocksize_group[] = {1};


  errorCode = clEnqueueNDRangeKernel(cmdQueue_cpu, kernel_cpu_sort, 1, NULL, globalsize_group, blocksize_group, 0, NULL, NULL); CHECKERROR;
  clFinish(cmdQueue_cpu);

  errorCode = clSetKernelArg(kernel_cpu_avg2 , 0, sizeof(cl_mem), &devSortOutput_b); CHECKERROR;
  errorCode = clSetKernelArg(kernel_cpu_avg2 , 1, sizeof(cl_mem), &devAvgOutput_b); CHECKERROR;
	
	// size_t globalsize_avg[] = {256*50689};
 //    size_t blocksize_avg[] = {256};
    size_t globalsize_avg[] = {16};
    size_t blocksize_avg[] = {16};
	
    //while(GPU_available==false);
    //GPU_available=false;
 
      //double a = timestamp();
   // clFinish(cmdQueue_cpu);
	errorCode = clEnqueueNDRangeKernel(cmdQueue_cpu, kernel_cpu_avg2, 1, NULL, globalsize_avg, blocksize_avg, 0, NULL, NULL); CHECKERROR;
  clFinish(cmdQueue_cpu);
    //GPU_available=true;
     // double b = timestamp();
//cout<<"B gpu avg"<<(b-a)/1000000<<endl;
    env->ReleaseByteArrayElements(jdata, jbytedata, 0);
}

