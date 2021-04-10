#include<stdio.h>
#include<time.h>
#include<stdint.h>
#include<stdio.h>
#include<inttypes.h>


float rand_float(float s){
	return 4*s*(1-s);
}
void matrix_gen(FILE *fp,uint64_t data_size,float seed){
	float s=seed;
    printf("data_size is: %lu\n", data_size);
    
	for(uint64_t i=0; i<data_size; i++)
    {
		s=rand_float(s);
        fprintf(fp,"%f\n", s);
	}
}

int main(int argc, char *argv[]) {
    uint64_t data_size = 0x100;//256
    // uint64_t data_size = 0x10000000;//256M
    // uint64_t data_size = 0x40000000;//1G
    // uint64_t data_size = 0x100000000;//4G    
    FILE *fp;

    fp=fopen("char_test_data","w");
    // fp=fopen("256M_data","w");
    // fp=fopen("1G_data","w");
    // fp=fopen("4G_data","w");
    
    if(fp==NULL)  
    {  
        printf("File cannot open! " );  
        return 1;
    }  

    matrix_gen(fp, data_size, 0.1);

    fclose(fp);    
    return 0;
}

