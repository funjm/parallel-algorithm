#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "mpi.h"

#define BLOCK 1024*1024LL


double starttime , endtime;

int cmp(const void * a, const void * b) {
    if (*(int*)a < *(int*)b) return -1;
    if (*(int*)a > *(int*)b) return 1;
    else return 0;
}

int main(int argc, char *argv[]) {
    if(argc!=3)
    {
        printf("Useage: %s data datasize(*4MB)\n");
        exit(1);
    }

    //二进制浮点数文件名,由参数1传入
    char *data_file = (argv[1]);
    //待排序argv[2]M个浮点数
    uint64_t array_size = atoi(argv[2]) * BLOCK;

    //进程号
    int my_rank=0;
    //进程总数
    int comm_sz=0;

    //MPI初始化
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    //主机名
    int  nameLength;
    char processorName[MPI_MAX_PROCESSOR_NAME];
    MPI_Get_processor_name(processorName,&nameLength);
    printf("Process %d is on %s\n", my_rank, processorName);
    MPI_Barrier(MPI_COMM_WORLD);
    //所有待排序的数组指针,由进程0负责

    float* array = NULL;

    int64_t average_array_size = array_size/comm_sz;
    //初始均匀局部待待排序的数组指针,由各进程负责    
    // float* average_array=(float *) malloc(average_array_size*sizeof(float));
    float* average_array=new float[average_array_size];
    //由柵栏划分的局部待排序数组指针,由各进程负责 
    //!!!方便起见先分配整个文件大小
    // float* local_array=(float *) malloc(array_size*sizeof(float));
    float* local_array=(float *) malloc(array_size*sizeof(float)/2);

    //进程0读取数据
    if(my_rank == 0)
    {
        array = (float *) malloc(array_size*sizeof(float));

        printf("reading %s of %ldB, which contains %ldM of floats\n", data_file, array_size*sizeof(float), array_size);

        FILE *fp;
        fp=fopen(data_file,"rb");
        if(fp==NULL){
            printf("Cannot Open file \n");
            exit(1);
        }

        //每次读入16*4=48MB大小
        int read_size = 16 * BLOCK;
        //需要循环读取的次数
        int read_turn = array_size / read_size;
        printf("read turn: %d\n",read_turn);

        int count=0;
        for(int i=0; i<read_turn; i++)
        {
            count = fread(array+i*read_size, sizeof(float), read_size, fp);
        }

        fclose(fp);
        printf("finishing reading, the first data is %f, the last data is %f\n", array[0], array[array_size-1]);

        //输出最后5个验证非0性
        printf("print last 5 numbers to valid the data the read process\n");
        for(int i=array_size-5; i<array_size; i++)
        {
            printf("%f\n",array[i]);
        }        
        printf("\n");
    }

    MPI_Barrier(MPI_COMM_WORLD);
    // printf("total size is %ld, average size is %ld\n", array_size, average_array_size);
    //!!!听说scatter有问题,先将就
    //        原数组指针,     单进程接受数量,     数据类型,   接受数组的指针,    单进程接受数量,  数据类型,  执行进程, 对应的communicator
	MPI_Scatter(array, average_array_size, MPI_FLOAT, average_array, average_array_size, MPI_FLOAT, 0, MPI_COMM_WORLD);
    
 
    printf("---------------\n after Scatter Process %d is on %s\n", my_rank, processorName);
       /*test*/
    // if(my_rank ==  3)
    // {
    //     printf("Process 1's average array after Scatter\n");
    //     for(int i=average_array_size-5; i<average_array_size; i++)
    //     {
    //         printf("%f\n",average_array[i]);
    //     }
    // }

    // //p 个处理器 就要有P个数据项
	// uint64_t subArraySize, startIndex, endIndex;
    // int *partitionSizes, *newPartitionSizes;
	    
	// float *pivots, *newPartitions;
	

    // pivots = (float *) malloc(p*sizeof(float));
    // partitionSizes = (int *) malloc(p*sizeof(int));
    // newPartitionSizes = (int *) malloc(p*sizeof(int));
	
    // for ( k = 0; k < p; k++) {
    //     partitionSizes[k] = 0;
    // }

    // // 根据进程号获取每个进程各自的起始位置，子数组大小, 结束位置
    // startIndex = myId * N / p;
    // if (p == (myId + 1)) {
    //     endIndex = N;
    // } 
    // else {
    //     endIndex = (myId + 1) * N / p;
    // }
    // subArraySize = endIndex - startIndex;




    // MPI_Barrier(MPI_COMM_WORLD);
    // printf("start !\n");
    // //调用各阶段函数
	// starttime = MPI_Wtime();
	// //1.阶段1 选取出p个数据项 --> pivots
    // phase1(array, N, startIndex, subArraySize, pivots, p);
    // if (p > 1) {
	// //2.把pivots收集起来选取p-1个 分割点，每一段分割区间的大小放到partitionSizes中
    //   phase2(array, startIndex, subArraySize, pivots, partitionSizes, p, myId);
    // // //3.编号为k的处理器保持第k段数据，发送第j段数据到处理器j上
    //    phase3(array, startIndex, partitionSizes, &newPartitions, newPartitionSizes, p);
    // // //4.每个处理器上有p段数据，将它们归并为一个队列
    //    phase4(newPartitions, newPartitionSizes, p, myId, array);
    // }
    // endtime  = MPI_Wtime();

    // if (myId == 0){ //root节点输出排序值，和时间
    //     printf("spent time = %lf second\n", endtime - starttime);
    //     FILE * result ;
    //     result = fopen("result.txt","w+");
	// 	printf("write ...");
    //     for(int i=0;i<N;++i){
    //         fprintf(result,"%f ",array[i]);
    //     }
    //     printf("array[0] : %f\n",array[0]);
    //     printf("array[n-1] : %f\n",array[N-1]);
		
        
    // }

    // if (p > 1) {
    //   free(newPartitions);
    // }
    // free(partitionSizes);
    // free(newPartitionSizes);
    // free(pivots);
    free(array);
    MPI_Finalize();
    return 0;
}