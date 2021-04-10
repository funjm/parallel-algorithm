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
int i,j,k;

double starttime , endtime;

int cmp(const void * a, const void * b) {
  if (*(int*)a < *(int*)b) return -1;
  if (*(int*)a > *(int*)b) return 1;
  else return 0;
}

void phase1(float *array, uint64_t N, uint64_t startIndex, uint64_t subArraySize, float *pivots, int p) {
  // 对子数组进行局部排序
  qsort(array + startIndex, subArraySize, sizeof(array[0]), cmp);

  // 正则采样
  for (i = 0; i < p; i++) {
    pivots[i] = array[startIndex + (i * (N / (p * p)))];    
  }
  return;
}

void phase2(float *array, uint64_t startIndex, uint64_t subArraySize, float *pivots, int *partitionSizes, int p, int myId) {
  float *collectedPivots = (float *) malloc(p * p * sizeof(pivots[0]));
  float *phase2Pivots = (float *) malloc((p - 1) * sizeof(pivots[0]));          //主元
  int index = 0;

  //收集消息，根进程在它的接受缓冲区中包含所有进程的发送缓冲区的连接。
  MPI_Gather(pivots, p, MPI_FLOAT, collectedPivots, p, MPI_FLOAT, 0, MPI_COMM_WORLD);       
  if (myId == 0) {

    qsort(collectedPivots, p * p, sizeof(pivots[0]), cmp);          //对正则采样的样本进行排序

    // 采样排序后进行主元的选择
    for (i = 0; i < (p -1); i++) {
      phase2Pivots[i] = collectedPivots[(((i+1) * p) + (p / 2)) - 1];
    }
  }
  //发送广播
  MPI_Bcast(phase2Pivots, p - 1, MPI_FLOAT, 0, MPI_COMM_WORLD);
  // 进行主元划分，并计算划分部分的大小
  for ( i = 0; i < subArraySize; i++) {
    if (array[startIndex + i] > phase2Pivots[index]) {
      //如果当前位置的数字大小超过主元位置，则进行下一个划分
      index += 1;
    }
    if (index == p) {
      //最后一次划分，子数组总长减掉当前位置即可得到最后一个子数组划分的大小
      partitionSizes[p - 1] = subArraySize - i + 1;
      break;
    }
    partitionSizes[index]++ ;   //划分大小自增
  }
  free(collectedPivots);
  free(phase2Pivots);
  return;
}

void phase3(float *array, uint64_t startIndex, int *partitionSizes, float **newPartitions, int *newPartitionSizes, int p) {
  int totalSize = 0;
  int *sendDisp = (int *) malloc(p * sizeof(int));
  int *recvDisp = (int *) malloc(p * sizeof(int));

  // 全局到全局的发送，每个进程可以向每个接收者发送数目不同的数据.将第k段数据发给第k段进程，收集其他进程发来的本端数据
  MPI_Alltoall(partitionSizes, 1, MPI_INT, newPartitionSizes, 1, MPI_INT, MPI_COMM_WORLD);

  // 计算划分的总大小，并给新划分分配空间
  for ( i = 0; i < p; i++) {
    totalSize += newPartitionSizes[i];
  }
  *newPartitions = (float *) malloc(totalSize * sizeof(float));

  // 在发送划分之前计算相对于sendbuf的位移，此位移处存放着输出到进程的数据
  sendDisp[0] = 0;
  recvDisp[0] = 0;      //计算相对于recvbuf的位移，此位移处存放着从进程接受到的数据
  for ( i = 1; i < p; i++) {
    sendDisp[i] = partitionSizes[i - 1] + sendDisp[i - 1];
    recvDisp[i] = newPartitionSizes[i - 1] + recvDisp[i - 1];
  } 

  //发送数据，实现n次点对点通信
  MPI_Alltoallv(&(array[startIndex]), partitionSizes, sendDisp, MPI_FLOAT, *newPartitions, newPartitionSizes, recvDisp, MPI_FLOAT, MPI_COMM_WORLD);

  free(sendDisp);
  free(recvDisp);
  return;
}

void phase4(float *partitions, int *partitionSizes, int p, int myId, float *array) {
  float *sortedSubList;
  int *recvDisp, *partitionEnds, *subListSizes, totalListSize;
  int *indexes;

  indexes = (int *) malloc(p * sizeof(int));
  partitionEnds = (int *) malloc(p * sizeof(int));
  indexes[0] = 0;
  totalListSize = partitionSizes[0];
  for ( i = 1; i < p; i++) {
    totalListSize += partitionSizes[i];
    indexes[i] = indexes[i-1] + partitionSizes[i-1];
    partitionEnds[i-1] = indexes[i];
  }
  partitionEnds[p - 1] = totalListSize;

  sortedSubList = (float *) malloc(totalListSize * sizeof(float));
  subListSizes = (int *) malloc(p * sizeof(int));
  recvDisp = (int *) malloc(p * sizeof(int));

  // 归并排序
  for ( i = 0; i < totalListSize; i++) {
    float lowest = 3.14;
    int ind = -1;
    for (j = 0; j < p; j++) {
      if ((indexes[j] < partitionEnds[j]) && (partitions[indexes[j]] < lowest)) {
    lowest = partitions[indexes[j]];
    ind = j;
      }
    }
    sortedSubList[i] = lowest;
    indexes[ind] += 1;
  }

  // 发送各子列表的大小回根进程中
  MPI_Gather(&totalListSize, 1, MPI_FLOAT, subListSizes, 1, MPI_FLOAT, 0, MPI_COMM_WORLD);

  // 计算根进程上的相对于recvbuf的偏移量
  if (myId == 0) {
    recvDisp[0] = 0;
    for ( i = 1; i < p; i++) {
      recvDisp[i] = subListSizes[i - 1] + recvDisp[i - 1];
    }
  }

  //发送各排好序的子列表回根进程中
  MPI_Gatherv(sortedSubList, totalListSize, MPI_FLOAT, array, subListSizes, recvDisp, MPI_FLOAT, 0, MPI_COMM_WORLD);

  free(partitionEnds);
  free(sortedSubList);
  free(indexes);
  free(subListSizes);
  free(recvDisp);
  return;
}

//PSRS排序函数，调用了4个过程函数
void psrs_mpi(float *array, uint64_t N, char *filename)    
{
    int p, myId, nameLength;
	
    //p 个处理器 就要有P个数据项
	uint64_t subArraySize, startIndex, endIndex;
    int *partitionSizes, *newPartitionSizes;
	    
    
	float *pivots, *newPartitions;
	
    char processorName[MPI_MAX_PROCESSOR_NAME];


    MPI_Comm_size(MPI_COMM_WORLD,&p);
    MPI_Comm_rank(MPI_COMM_WORLD,&myId);
    MPI_Get_processor_name(processorName,&nameLength);

    printf("Process %d is on %s\n",myId, processorName);

    pivots = (float *) malloc(p*sizeof(float));
    partitionSizes = (int *) malloc(p*sizeof(int));
    newPartitionSizes = (int *) malloc(p*sizeof(int));
	
    for ( k = 0; k < p; k++) {
      partitionSizes[k] = 0;
    }

    // 根据进程号获取每个进程各自的起始位置，子数组大小, 结束位置
    startIndex = myId * N / p;
    if (p == (myId + 1)) {
      endIndex = N;
    } 
    else {
      endIndex = (myId + 1) * N / p;
    }
    subArraySize = endIndex - startIndex;

    if(myId == 0)
    {
        int fp;
        fp=open(filename,O_RDONLY);
        if(fp==-1){
            printf("Cannot Open file \n");
            exit(1);
        }   
        read(fp, array, N * sizeof( float));
        close(fp);
    }


    MPI_Barrier(MPI_COMM_WORLD);
    printf("start !\n");
    //调用各阶段函数
	starttime = MPI_Wtime();
	//1.阶段1 选取出p个数据项 --> pivots
    phase1(array, N, startIndex, subArraySize, pivots, p);
    if (p > 1) {
	//2.把pivots收集起来选取p-1个 分割点，每一段分割区间的大小放到partitionSizes中
      phase2(array, startIndex, subArraySize, pivots, partitionSizes, p, myId);
    // //3.编号为k的处理器保持第k段数据，发送第j段数据到处理器j上
       phase3(array, startIndex, partitionSizes, &newPartitions, newPartitionSizes, p);
    // //4.每个处理器上有p段数据，将它们归并为一个队列
       phase4(newPartitions, newPartitionSizes, p, myId, array);
    }
    endtime  = MPI_Wtime();

    if (myId == 0){ //root节点输出排序值，和时间
        printf("spent time = %lf second\n", endtime - starttime);
        FILE * result ;
        result = fopen("result.txt","w+");
		printf("write ...");
        for(int i=0;i<N;++i){
            fprintf(result,"%f ",array[i]);
        }
        printf("array[0] : %f\n",array[0]);
        printf("array[n-1] : %f\n",array[N-1]);
		
        
    }

    if (p > 1) {
      free(newPartitions);
    }
    free(partitionSizes);
    free(newPartitionSizes);
    free(pivots);


  free(array);
  MPI_Finalize();

}

int main(int argc, char *argv[]) {

	float *array;
    uint64_t N = atoi(argv[2]) * BLOCK;
	array = (float *) malloc(N * sizeof(float) );
/*
    srand(100);
    for ( k = 0; k < N; k++) {
      array[k] = rand()%100;
    }
*/
    MPI_Init(&argc,&argv);      //MPI初始化
    psrs_mpi(array,N,argv[1]);          //调用PSRS算法进行并行排序

  return 0;
}