#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map <string, Block> node_blocks;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status) {

    //TODO: Enviar mensaje TAG_CHAIN_HASH

    Block *blockchain = new Block[VALIDATION_BLOCKS];

    //TODO: Recibir mensaje TAG_CHAIN_RESPONSE

    //TODO: Verificar que los bloques recibidos
    //sean válidos y se puedan acoplar a la cadena
    //delete []blockchain;
    //return true;


    delete[]blockchain;
    return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status) {
    if (valid_new_block(rBlock)) {

        //Agrego el bloque al diccionario, aunque no
        //necesariamente eso lo agrega a la cadena
        node_blocks[string(rBlock->block_hash)] = *rBlock;

        //Si el índice del bloque recibido es 1
        //y mí último bloque actual tiene índice 0,
        //entonces lo agrego como nuevo último.
        if (rBlock->index == 1 && last_block_in_chain->index == 0) {
            *last_block_in_chain = *rBlock;
            printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
            return true;
        }

        //Si el índice del bloque recibido es
        //el siguiente a mí último bloque actual,
        //y el bloque anterior apuntado por el recibido es mí último actual,
        //entonces lo agrego como nuevo último.
        if ((rBlock->index == last_block_in_chain->index + 1) && rBlock->previous_block_hash == last_block_in_chain->block_hash) {
            *last_block_in_chain = *rBlock;
            printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
            return true;
        }

        //Si el índice del bloque recibido es
        //el siguiente a mí último bloque actual,
        //pero el bloque anterior apuntado por el recibido no es mí último actual,
        //entonces hay una blockchain más larga que la mía.
        if ((rBlock->index == last_block_in_chain->index + 1) && rBlock->previous_block_hash != last_block_in_chain->block_hash) {
            printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
            bool res = verificar_y_migrar_cadena(rBlock, status);
            return res;
        }

        //Si el índice del bloque recibido es igual al índice de mi último bloque actual,
        //entonces hay dos posibles forks de la blockchain pero mantengo la mía
        if (rBlock->index == last_block_in_chain->index) {
            printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n", mpi_rank, rBlock->index,
                   status->MPI_SOURCE);
            return false;
        }

        //Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
        //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
        if (rBlock->index < last_block_in_chain->index) {
            printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n", mpi_rank, rBlock->index,
                   last_block_in_chain->index, status->MPI_SOURCE);
            return false;
        }

        //Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
        //entonces me conviene abandonar mi blockchain actual
        if (rBlock->index > last_block_in_chain->index+1) {
            printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
            bool res = verificar_y_migrar_cadena(rBlock,status);
            return res;
        }

    }

    printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n", mpi_rank, status->MPI_SOURCE);
    return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block) {
    //No enviar a mí mismo
    for (int destination = mpi_rank+1; destination < total_nodes; ++destination) {
        MPI_Datatype datatype;
        define_block_data_type_for_MPI(&datatype);
        MPI_Send((void*) block,
            1,
            datatype,
            destination,
            TAG_NEW_BLOCK,
            MPI_COMM_WORLD);        
    }
    for (int destination = 0; destination < mpi_rank; ++destination)
    {
        MPI_Datatype datatype;
        define_block_data_type_for_MPI(&datatype);
        MPI_Send((void*) block,
            1,
            datatype,
            destination,
            TAG_NEW_BLOCK,
            MPI_COMM_WORLD);
    }
}

//Proof of work
//Advertencia: puede tener condiciones de carrera
void *proof_of_work(void *ptr) {
    pthread_mutex_t * lock = (pthread_mutex_t*) ptr;
    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while (true) {

        block = *last_block_in_chain;

        //Preparar nuevo bloque
        block.index += 1;
        block.node_owner_number = mpi_rank;
        block.difficulty = DEFAULT_DIFFICULTY;
        memcpy(block.previous_block_hash, block.block_hash, HASH_SIZE);

        //Agregar un nonce al azar al bloque para intentar resolver el problema
        gen_random_nonce(block.nonce);

        //Hashear el contenido (con el nuevo nonce)
        block_to_hash(&block, hash_hex_str);

        //Contar la cantidad de ceros iniciales (con el nuevo nonce)
        if (solves_problem(hash_hex_str)) {

            pthread_mutex_lock(lock);
            //Verifico que no haya cambiado mientras calculaba
            if (last_block_in_chain->index < block.index) {
                mined_blocks += 1;
                *last_block_in_chain = block;
                strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
                last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
                node_blocks[hash_hex_str] = *last_block_in_chain;
                printf("[%d] Agregué un producido con index %d \n", mpi_rank, last_block_in_chain->index);

                //Mientras comunico, no responder mensajes de nuevos nodos
                broadcast_block(last_block_in_chain);
            }
            pthread_mutex_unlock(lock);
        }

    }

    return NULL;
}


int node() {

    pthread_mutex_t lock;

    pthread_mutex_init(&lock, NULL);

    //Tomar valor de mpi_rank y de nodos totales
    MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    //La semilla de las funciones aleatorias depende del mpi_ranking
    srand(time(NULL) + mpi_rank);
    printf("[MPI] Lanzando proceso %u\n", mpi_rank);

    last_block_in_chain = new Block;

    //Inicializo el primer bloque
    last_block_in_chain->index = 0;
    last_block_in_chain->node_owner_number = mpi_rank;
    last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
    last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
    memset(last_block_in_chain->previous_block_hash, 0, HASH_SIZE);

    //Crear thread para minar
    pthread_t thread;
    pthread_create(&thread, NULL, proof_of_work, &lock);

    while (true) {
        pthread_mutex_lock(&lock);

        Block *rBlock = new Block;
        MPI_Datatype datatype;
        define_block_data_type_for_MPI(&datatype);
        MPI_Status *status = new MPI_Status;
        //TODO: Recibir mensajes de otros nodos
        MPI_Recv((void*) rBlock,
                1,
                datatype,
                MPI_ANY_SOURCE,
                MPI_ANY_TAG,
                MPI_COMM_WORLD,
                status);

        //TODO: Si es un mensaje de nuevo bloque, llamar a la función
        if (status->MPI_TAG == TAG_NEW_BLOCK)
            validate_block_for_chain(rBlock, status);

        //TODO: Si es un mensaje de pedido de cadena,
        if (status->MPI_TAG == TAG_CHAIN_HASH)
            // recorrer la blockchain para atras mandando los hashes
        

        pthread_mutex_unlock(&lock);
    }

    delete last_block_in_chain;
    return 0;
}