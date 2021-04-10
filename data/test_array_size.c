#include<stdio.h>
#include<time.h>
#include<stdint.h>
#include <stdlib.h>
#include<inttypes.h>


int main(int argc, char *argv[]) 
{
    // uint64_t data_size = 0x10000000;//16**7 * 4B = 1GB
    // uint64_t data_size = 0x5040000; //free yes
    // uint64_t data_size = 0xad50000;//avaliable 628M

    uint64_t data_size = 0x8000000;//256M

    // float float_array[(data_size)]; 
    printf("allocate %luB memory to float array\n", data_size);
    float* float_array= (float*)malloc(sizeof(float)*data_size);
    return 0;
}