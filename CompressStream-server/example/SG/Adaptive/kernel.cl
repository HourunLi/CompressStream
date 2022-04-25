#pragma OPENCL EXTENSION cl_amd_printf : enable
//#define VEC2DWIDTH 1024
//#define WARPSIZE 64
//#define WARPSIZE 32
//#define WORKGROUP_SIZE 256
//#define TUPLE_SIZE 32
//#define TUPLE_SIZE 28
//#define OUTPUT_TUPLE_SIZE 12
//#define INPUT_VECTOR_SIZE 4
//#define OUTPUT_VECTOR_SIZE 1
//#define TUPLE_NUM 3600
//#define WINDOW_SIZE 36000
//#define WINDOW_SIZE 12*3600 
//60 windws each is 16 bytes
#define TUPLE_NUM_ALL 50689
#define WINDOW_NUM 100
#define TUPLE_NUM 1024
#define TUPLE_SIZE 25
#define G_TUPLE_NUM 1024
#define P_TUPLE_SIZE_1 12
#define P_TUPLE_SIZE_2 21
#define G_WINDOW_NUM 50689
typedef struct {
  long t;
  float _1;
  int _2;
  char _3;
  int _4;
  int _5;
  //int tmp;
} tuple_1 __attribute__((aligned(1)));

typedef union {
  tuple_1 tuple;
  uchar char_data[25];
} tuple_a;

typedef struct {
  long t;
  float _1;
} tuple_2 __attribute__((aligned(1)));

typedef union {
  tuple_2 tuple;
  uchar char_data[12];
} tuple_b;

typedef struct {
  long t;
  float _1;
  char _2;
  int _3;
  int _4;
} tuple_3 __attribute__((aligned(1)));

typedef union {
  tuple_3 tuple;
  uchar char_data[21];
} tuple_c;


__kernel void cpu_projection1(

    __global uchar *input,  
    __global uchar *output)
{

    int globalID = get_global_id(0);

  int start=globalID*WINDOW_NUM*TUPLE_NUM/3;
  int end=(globalID+1)*WINDOW_NUM*TUPLE_NUM/3;

  for(int k=start; k<end; k++){

    for(int i=0; i<P_TUPLE_SIZE_1; i++)
      output[k*P_TUPLE_SIZE_1+i]=input[k*P_TUPLE_SIZE_1 +i];
  }

}


float bytesToFloat(char buffer1,char buffer2,char buffer3,char buffer4) {
    float res;
    *((unsigned char *) (&res) + 3) = buffer1;
    *((unsigned char *) (&res) + 2) = buffer2;
    *((unsigned char *) (&res) + 1) = buffer3;
    *((unsigned char *) (&res) + 0) = buffer4;
    return res;
}

void floatToBytes(char * res ,float f) {
    
    res[0] = *((unsigned char *) (&f) + 3);
    res[1] = *((unsigned char *) (&f) + 2);
    res[2] = *((unsigned char *) (&f) + 1);
    res[3] = *((unsigned char *) (&f) + 0);
}



__kernel void cpu_avg1(

    __global uchar *input,  
    __global uchar *output)
{
     
  int globalID = get_global_id(0);
  int localID = get_local_id(0);
  int groupID = get_group_id(0);
  int groupSize = get_local_size(0);
  int input_idx = groupID*12;
  __global uchar* _input = &input[input_idx];
 
 
  float sum=0;

  //if(localID==0){

    for(int i=0; i<G_TUPLE_NUM; i++ ){//workgroup related section
 
      float relatedValue  = bytesToFloat(_input[i*12+8] , _input[i*12+9], _input[i*12+10] , _input[i*12+11]);
      sum += relatedValue;

    }
    

    float avg = sum/512;
   
    int output_idx = groupID*12;
    for(int i=0; i<8; i++)
       output[output_idx+i]=input[globalID*12 + i];
   
    char tmp[4] ;
    floatToBytes(tmp,avg);

    output[output_idx+8]=tmp[0];
    output[output_idx+9]=tmp[1];
    output[output_idx+10]=tmp[2];
    output[output_idx+11]=tmp[3];


 }

 __kernel void cpu_projection2(

    __global uchar *input,  
    __global uchar *output)
{
    int globalID = get_global_id(0);

  int start=globalID*WINDOW_NUM*TUPLE_NUM/3;
  int end=(globalID+1)*WINDOW_NUM*TUPLE_NUM/3;


  for(int k=start; k<end; k++){

    for(int i=0; i<12; i++)
      output[k*P_TUPLE_SIZE_2+i]=input[k*TUPLE_SIZE+i];
    for(int i = 12; i < 21; i++)
      output[k*P_TUPLE_SIZE_2+i]=input[k*TUPLE_SIZE+i+4];
  }

}

__kernel void cpu_avg2(

    __global uchar *input,  
    __global uchar *output)
{
     
  int globalID = get_global_id(0);
  int localID = get_local_id(0);
  int groupID = get_group_id(0);
  int groupSize = get_local_size(0);
  int input_idx = groupID*21;
  __global uchar* _input = &input[input_idx];
 
 
  float sum=0;

    for(int i=0; i<G_TUPLE_NUM; i++ ){//workgroup related section
 
      float relatedValue  = bytesToFloat(_input[i*21+8] , _input[i*21+9], _input[i*21+10] , _input[i*21+11]);
      sum += relatedValue;

    }
    

    float avg = sum/512;
   
    int output_idx = groupID*21;
    for(int i=0; i<8; i++)
      output[output_idx+i]=input[globalID*21 + i];
    for(int i=8; i<17; i++)
      output[output_idx+i]=input[globalID*21 + 4 + i];
   
    char tmp[4] ;
    floatToBytes(tmp,avg);

    output[output_idx+17]=tmp[0];
    output[output_idx+18]=tmp[1];
    output[output_idx+19]=tmp[2];
    output[output_idx+20]=tmp[3];


 }
 
__kernel void cpu_sort(
    __global uchar *input,
    __global uchar *output
    ){
  int localID = get_local_id(0);
  int groupID = get_group_id(0);
  int groupSize = get_local_size(0);
  int input_idx = groupID*P_TUPLE_SIZE_2;
  int output_idx = groupID*P_TUPLE_SIZE_2*G_TUPLE_NUM;
  __global uchar* _window = &output[output_idx];


  for(int i=0; i<TUPLE_NUM*P_TUPLE_SIZE_2; i++ ){
    _window[i] = input[input_idx + i];
  }

  for (unsigned int k = 2; k <= G_TUPLE_NUM ; k *= 2){  
    for (unsigned int j = k / 2; j>0; j /= 2){  

      int thread = localID;
      for(localID=0; localID<G_TUPLE_NUM; localID++){

        unsigned int ixj = localID ^ j;   
        __global tuple_c* pixj = (__global tuple_c*)&_window[ixj*P_TUPLE_SIZE_2];   
        if (ixj > localID ){    
          __global tuple_c* pLocalID = (__global tuple_c*)&_window[localID*P_TUPLE_SIZE_2];   
          if ((localID & k) == 0){        
            if (pLocalID->tuple._2 > pixj->tuple._2){     
              tuple_3 tupleTmp = pLocalID->tuple;
              pLocalID->tuple = pixj->tuple;
              pixj->tuple = tupleTmp;
            }
          }   
          else{         
            if (pLocalID->tuple._2 < pixj->tuple._2){     
              tuple_3 tupleTmp = pLocalID->tuple;
              pLocalID->tuple = pixj->tuple;
              pixj->tuple = tupleTmp;
            }     
          }   
        }   
      }   
      barrier(CLK_LOCAL_MEM_FENCE);
      localID=thread;
    } 
  }  
}


