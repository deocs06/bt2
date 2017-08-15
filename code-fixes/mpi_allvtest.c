#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int MPI_Alltoallv_fix_buserror(void *sendbuf, int *sendcnts, int *sdispls,
        MPI_Datatype sendtype, void *recvbuf, int *recvcnts,
        int *rdispls, MPI_Datatype recvtype, MPI_Comm comm);

void readidfile(unsigned long int *sendbuf,int ThisTask);

int MPI_Alltoallv_fix_buserror(void *sendbuf, int *sendcnts, int *sdispls,
        MPI_Datatype sendtype, void *recvbuf, int *recvcnts,
        int *rdispls, MPI_Datatype recvtype, MPI_Comm comm)
{
   int i, j, ncut;
   int ThisTask, NTask;
   int *sendcnts_ncut, *sdispls_ncut, *recvcnts_ncut, *rdispls_ncut;
   MPI_Comm_rank(comm, &ThisTask);
   MPI_Comm_size(comm, &NTask);

   ncut = 4;
   sendcnts_ncut = (int *) malloc(NTask * sizeof(int));
   recvcnts_ncut = (int *) malloc(NTask * sizeof(int));
   sdispls_ncut = (int *) malloc(NTask * sizeof(int));
   rdispls_ncut = (int *) malloc(NTask * sizeof(int));
   for (i=0; i<NTask; i++){
        sendcnts_ncut[i] = sendcnts[i]/ncut;
        recvcnts_ncut[i] = recvcnts[i]/ncut;
        sdispls_ncut[i] = sdispls[i];
        rdispls_ncut[i] = rdispls[i];
   }
   for (j =0; j<ncut;j++){
       if (j == ncut-1){
           for (i=0; i<NTask; i++){
               sendcnts_ncut[i] = sendcnts[i] - j*sendcnts_ncut[i];   
               recvcnts_ncut[i] = recvcnts[i] - j*recvcnts_ncut[i];
           }
       } 
       MPI_Alltoallv(sendbuf, sendcnts_ncut, sdispls_ncut, sendtype, recvbuf, recvcnts_ncut, rdispls_ncut, recvtype, comm);
       for (i=0; i<NTask; i++){
           sdispls_ncut[i] = sdispls_ncut[i] + sendcnts_ncut[i];   
           rdispls_ncut[i] = rdispls_ncut[i] + recvcnts_ncut[i];
       }
   }
   return 1;       
}

int main(int argc, char **argv)
{
    int i, ThisTask, NTask;
    unsigned long int *sendbuf, *recvbuf;
    int *scounts, *sdisp, *rcounts, *rdisp;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &ThisTask);
    MPI_Comm_size(MPI_COMM_WORLD, &NTask);
    scounts = (int *) malloc(NTask * sizeof(int));
    rcounts = (int *) malloc(NTask * sizeof(int));
    for (i=0;i<NTask;i++){
            scounts[i] = (int) 100;
            rcounts[i] = (int) 100;
    }
    sdisp = (int *) malloc(NTask * sizeof(int));
    rdisp = (int *) malloc(NTask * sizeof(int));
    sdisp[0] = 0;
    rdisp[0] = 0;
    for (i=1;i<NTask;i++){
            sdisp[i] = scounts[i-1] + sdisp[i-1];
            rdisp[i] = rcounts[i-1] + rdisp[i-1];
    }
    sendbuf = (unsigned long int *) malloc (32*100 * sizeof(unsigned long int));
    recvbuf = (unsigned long int *) malloc (32*100 * sizeof(unsigned long int));
    readidfile(sendbuf,ThisTask);
    // MPI_Alltoallv(sendbuf, scounts, sdisp, MPI_UNSIGNED_LONG, recvbuf, rcounts, rdisp, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
    MPI_Alltoallv_fix_buserror(sendbuf, scounts, sdisp, MPI_UNSIGNED_LONG, recvbuf, rcounts, rdisp, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
    printf("Tasks %d from proc %d is initial %lu, final %lu \n",NTask,ThisTask,recvbuf[0],recvbuf[32*100 - 1]);
    MPI_Finalize(); 
    return 0;
}

void readidfile(unsigned long int *sendbuf,int ThisTask)
{
     FILE *ptr_myfile;
     ptr_myfile=fopen("/home/snapdir_data/snapdir_data/snappid_notsorted","rb");
     fseek(ptr_myfile, 32*100*ThisTask*sizeof(unsigned long int), SEEK_SET);
     fread(sendbuf,sizeof(unsigned long int),3200,ptr_myfile);
}

