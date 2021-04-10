#include<stdio.h>
#include<time.h>
#include<stdint.h>
#include <stdlib.h>
#include<inttypes.h>


float rand_float(float s){
	return 4*s*(1-s);
}

void matrix_gen(float *a, uint64_t data_size, float* seed)
{
	float* s=seed;
    
	for(uint64_t i=0; i<data_size; i++)
    {
		*s = rand_float(*s);
        a[i] = *s;
	}
}

int main(int argc, char *argv[]) 
{
    // uint64_t data_size = 0x100;//256
    uint64_t array_size = 0x10000000;//256M
    // uint64_t array_size = 0x40000000;//1G
    // uint64_t data_size = 0x100000000;//4G   
    
    // uint64_t array_size = 0x8000000;//256M

    FILE *fp;
    // fp=fopen("test_data","wb");
    // fp=fopen("256M_data","wb");
    fp=fopen("1G_data","wb");
    // fp=fopen("4G_data","wb");    
    if(fp==NULL)  
    {  
        printf("File cannot open! " );  
        return 1;
    }     

    printf("allocate %luB memory to float array\n", array_size);
    // float float_array[data_size];
    float* float_array= (float*)malloc(sizeof(float)*array_size);
 
    float a = 0.1;
    float* seed = &a;

    for(int i=0; i<4; i++)
    {   
        printf("seed is :%f\n", *seed);
        // 生成数据放入float_array中
        matrix_gen(float_array, array_size, seed);
    
        // 将float_array写入文件
        fwrite(float_array, sizeof(float), array_size, fp);
    }
    

    fclose(fp);    
    return 0;
}