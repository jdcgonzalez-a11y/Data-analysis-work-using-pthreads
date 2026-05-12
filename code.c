#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "cJSON.h"

// Estrutura para representar uma medição individual
typedef struct {
    char cidade[50];
    char data_hora[30];
    double temperatura;
    double umidade;
    double pressao;
    double bateria;
    int spreading_factor;
} Medicao;

// Buffer compartilhado para o modelo Produtor-Consumidor
#define BUFFER_SIZE 1000
Medicao buffer[BUFFER_SIZE];
int count = 0;
int processamento_concluido = 0;

pthread_mutex_t mutex_buffer = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_cheio = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_vazio = PTHREAD_COND_INITIALIZER;

// --- Thread 1: Leitura e Limpeza ---
void* thread_leitura(void* arg) {
    char *filename = (char*)arg;
    FILE *f = fopen(filename, "rb");
    if (!f) return NULL;

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    fclose(f);

    cJSON *root = cJSON_Parse(data);
    const cJSON *item = NULL;

    cJSON_ArrayForEach(item, root) {
        // Extrai a string interna (payload ou brute_data)
        cJSON *payload_str = cJSON_GetObjectItem(item, "payload");
        if (!payload_str) payload_str = cJSON_GetObjectItem(item, "brute_data");

        if (payload_str) {
            cJSON *payload = cJSON_Parse(payload_str->valuestring);
            
            Medicao m;
            strcpy(m.cidade, cJSON_GetObjectItem(payload, "device_name")->valuestring);
            
            // Aqui você deve iterar sobre o array "data" interno para preencher m.temperatura, etc.
            // TODO: Lógica de eliminação de duplicatas aqui
            
            pthread_mutex_lock(&mutex_buffer);
            while (count == BUFFER_SIZE) pthread_cond_wait(&cond_vazio, &mutex_buffer);
            
            buffer[count++] = m;
            
            pthread_cond_signal(&cond_cheio);
            pthread_mutex_unlock(&mutex_buffer);
            
            cJSON_Delete(payload);
        }
    }
    
    pthread_mutex_lock(&mutex_buffer);
    processamento_concluido = 1;
    pthread_cond_broadcast(&cond_cheio);
    pthread_mutex_unlock(&mutex_buffer);

    free(data);
    cJSON_Delete(root);
    return NULL;
}

// --- Thread 2: Estatísticas ---
void* thread_estatisticas(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex_buffer);
        while (count == 0 && !processamento_concluido) pthread_cond_wait(&cond_cheio, &mutex_buffer);
        
        if (count == 0 && processamento_concluido) {
            pthread_mutex_unlock(&mutex_buffer);
            break;
        }

        Medicao m = buffer[--count];
        pthread_cond_signal(&cond_vazio);
        pthread_mutex_unlock(&mutex_buffer);

        // TODO: Atualizar Mínimos, Máximos e Médias globais aqui
    }
    return NULL;
}

// --- Thread 3: Logs ---
void* thread_logs(void* arg) {
    FILE *log_file = fopen("processamento.log", "w");
    // TODO: Implementar fila de mensagens para log
    fclose(log_file);
    return NULL;
}

int main() {
    pthread_t t1, t2, t3;
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    // Inicia com um dos arquivos (ajuste conforme necessário)
    pthread_create(&t1, NULL, thread_leitura, "mqtt_senzemo_cx_bg.json");
    pthread_create(&t2, NULL, thread_estatisticas, NULL);
    pthread_create(&t3, NULL, thread_logs, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);

    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("\nTempo total de execução: %.2f segundos\n", elapsed);

    return 0;
}