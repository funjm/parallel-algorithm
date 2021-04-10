#include <stdio.h>
#include <sys/types.h>   
#include <unistd.h>

#define FILE_SIZE 

#define FLOAT_ARRAY_SIZE 10

// useage: ./read source_file target_file

int main(int argc, char *argv[])
{
    if(argc!=3)
    {
        printf("Useage:%s [the file you want to check] [the file record the last 10 num]", argv[0]);
    }
    FILE *sfp;
    FILE *tfp;
    printf("read last 10 numbers\n");

    // "ab+":为在文件尾巴读和写而打开或创建
    sfp=fopen(argv[1], "ab+");
    tfp=fopen(argv[2], "w");

    long offset = 4 * FLOAT_ARRAY_SIZE;
    if(fseek(sfp, -offset, SEEK_END) == -1)
    {
        printf("fseek error\n");
    }

    float  float_array[FLOAT_ARRAY_SIZE];
    fread(float_array, sizeof(float), FLOAT_ARRAY_SIZE, sfp);
    for(int i=0; i<FLOAT_ARRAY_SIZE; i++)
    {
        // 输出最后FLOAT_ARRAY_SIZE个随机数
        printf("%f\n", float_array[i]);

        // 将其写入target_file
        fprintf(tfp,"%f\n", float_array[i]);
    }

    fclose(sfp);   
    fclose(tfp);   
    return 0;
}