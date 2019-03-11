// Systemy rozproszone 2019 - Patryk Wegrzyn
 
// Dwie wersje, jedna pozwala na odlaczanie sie klientow i ich powrotne dolaczanie
// ale nie wspiera dynamicznego dodawania nowych klientow, druga wersja
// (allow_broken_chain -> no) nie pozwala na odlaczanie klientow, ale wspiera
// dynamiczne dodawanie nowych klientow. Wybor wersji jest podawany przez uzytkownika
// po starcie aplikacji; w obrebie danego tokenringu moga dzialac tylko klienci,
// ktorzy maja jednakowa wartosc parametru allow_broken_chain.

// System eliminuje sytuacje, w ktorej wiadomosc krazylaby po sieci w nieskonczonosc
// (jesli wyslana wiadomosc dotrze do nadawcy bez potwierdzenia type != RCVD to token
// zmienia sie na pusty)

// System eliminuje sytuacje, w ktorej klienci mogliby zostac zagladzani (po wyslaniu
// wiadomosci kazdy klient musi zrzec sie mozliwosci wysylania kolejnej wiadomosci na czas 
// jednego pelnego obiegu tokenu)

#define _BSD_SOURCE
#define _DEFAULT_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>    
#include <ctype.h>
#include <endian.h>
#include <sys/epoll.h>
#include <signal.h>
#include <time.h>

#ifndef UNIX_PATH_MAX
#define UNIX_PATH_MAX 108
#endif

#define MAX_CLIENT_ID 16
#define MESSAGE_BUFFER_SIZE 64
#define LOG_MESSAGE_SIZE 128
#define LOGGERS_PORT 29420
#define LOGGERS_GROUP "225.0.0.37"

// Type of the connection used by a particular client process
typedef enum connection_t_tag {
    TCP,
    UDP
} connection_t;

// Message type
typedef enum message_type_tag {
    EMPTY,
    FULL,
    RCVD,
    CTRL,
    HELLO,
    RELINK
} message_type;

// Message struct
typedef struct __attribute__((__packed__)) message_tag {
    char receiver[MAX_CLIENT_ID];
    char sender[MAX_CLIENT_ID];
    char buffer[MESSAGE_BUFFER_SIZE];
    char predecessor[MAX_CLIENT_ID];
    char relinked[32];
} message;

// Message header struct
typedef struct __attribute__((__packed__)) message_header_tag {
    char receiver[MAX_CLIENT_ID];
    char sender[MAX_CLIENT_ID];
    char predecessor[MAX_CLIENT_ID];
} message_header;

// Main token
typedef struct __attribute__((__packed__)) token_tag {
    uint8_t type;
    uint16_t length;
    message payload;
} token;

// Message queue item
struct queue_item {
	message *msg;
	struct queue_item *next;
};

// Message queue
typedef struct message_queue_tag {
	struct queue_item *head;
	struct queue_item *tail;
} message_queue;

// ---------------------------------------------------------------------------

int socket_desc_in = -1;
int socket_desc_out;
int socket_desc_in_joining = -1;
int connected_with_server = 0;
int connected_with_client = 0;
message_queue queue;
pthread_t input_handle_thread;
pthread_t server_thread, client_thread;
pthread_mutex_t message_mutex = PTHREAD_MUTEX_INITIALIZER;
token to_send;
int should_send_msg;
char *IPbuffer; 
char *client_id, *ip_addr_human_neighbour;
in_port_t listening_port, neighbour_port;
connection_t connection;
int has_token;
int allow_broken_chain = 1;
struct in_addr neighbour_ip_addr;
struct sockaddr_in loggers_addr;
int loggers_socket;
int was_token_received = 0;
int should_quit = 0;
int has_my_successor_reconnected = 0;
int reconnecting = 0;
char predecessor_name[MAX_CLIENT_ID];
int should_change_address = 0;
in_port_t neighbour_port_relinked;
struct in_addr neighbour_ip_addr_relinked;
token relink_token;
int should_send_relink_info = 0;
int was_hello_send = 0;
token token_send;
token token_rcv;
int switch_to_predecessor = 0;
int need_to_skip_turn = 0;
struct sockaddr_in     server_addr;
struct sockaddr_in     client_addr;
struct sockaddr_in     new_client_addr;

// ---------------------------------------------------------------------------

// Get local hostname
void checkHostName(int hostname) 
{ 
    if (hostname == -1) 
    { 
        perror("gethostname"); 
        exit(1); 
    } 
} 
  
// Get host info
void checkHostEntry(struct hostent * hostentry) 
{ 
    if (hostentry == NULL) 
    { 
        perror("gethostbyname"); 
        exit(1); 
    } 
} 
  
// Safe IP convert
void checkIPbuffer(char *IPbuffer) 
{ 
    if (NULL == IPbuffer) 
    { 
        perror("inet_ntoa"); 
        exit(1); 
    } 
} 

// Message queue initialization
void init_message_queue(message_queue *q)
{
	q->head = q->tail = NULL;
}

// Check if queue is empty
int is_message_queue_empty(message_queue *q) 
{
    return q->head == NULL && q->tail == NULL;
}

// Message queue push
void push_message_queue(message_queue *q, message *a)
{
	struct queue_item *item = malloc(sizeof(item));
    if(item == NULL)
    {
        fprintf(stderr, "Error while allocating memory\n");
        exit(EXIT_FAILURE);
    }
	item->msg = a;
	item->next = NULL;
	if (q->head == NULL) q->head = q->tail = item;
    else q->tail = q->tail->next = item;
}

// Message queue pop
message* pop_message_queue(message_queue *q)
{
	message *top;
	if (q->head == NULL) return NULL;
    else 
    {
		top = q->head->msg;
		 struct queue_item *next = q->head->next;
		free(q->head);
		q->head = next;
		if (q->head == NULL) q->tail = NULL;
	}
	return top;
}

// Prints help info and quits the app when wrongs arguments have been detected
void sig_arg_err()
{
    printf("Wrong argument format.\n"
           "Usage: client <identifier> <port> <neighbour's_address:port> <TOKEN|NOTOKEN> <TCP|UDP>\n");
    exit(EXIT_FAILURE);
}

// Handler for the SIGINT interrupt
void handler_sigint(int signo)
{
    const char *info = "\nReceived a SIGINT interrupt. Quitting...\n";
    
    if(signo == SIGINT)
    {
        write(1, info, strlen(info));
        should_quit = 1;
    }
}

// Parses a given message from the user
message* parse_message(const char *as_text)
{
    message *msg;   
    msg = (message *)malloc(sizeof(message));
    if(msg == NULL)
    {
        fprintf(stderr, "Error while allocating memory\n");
        exit(EXIT_FAILURE);
    }

    if(sscanf(as_text, "%s -> %s", msg->receiver, msg->buffer) != 2)
    {
        if(strlen(msg->receiver) == 0) return NULL;
        else
        {
            fprintf(stderr, "Format: RECEIVER_CLIENT_ID -> MESSAGE_TO_BE_SENT\n");
            return NULL;
        }
    }

    sprintf(msg->sender, "%s", client_id);

    return msg;   
}

// Thread task responsible for reading input from the user
void* input_handle_task(void *args)
{   
    char user_input[MESSAGE_BUFFER_SIZE];
    message *msg;

    if(allow_broken_chain) 
    {
        printf("Waiting for neighbours to connect...\n");
        while(!connected_with_client || !connected_with_server) {
            sleep(1);
        }
    }

    while(1) 
    {
        printf("%s@%s:%d>> ", client_id, IPbuffer, (int)listening_port);
        fgets(user_input, MESSAGE_BUFFER_SIZE, stdin);
        if(user_input[strlen(user_input) - 1] == '\n')
        {
            user_input[strlen(user_input) - 1] = '\0';
        }
    
        if((msg=parse_message(user_input)) == NULL)
        {
            continue;
        }
    
        pthread_mutex_lock(&message_mutex);
        push_message_queue(&queue, msg);
        pthread_mutex_unlock(&message_mutex);
        fprintf(stdout, "Message has been succesfully added to the queue.\n");
    }
    
    return NULL;
}

// End of program cleanup
void perform_cleanup(void)
{
    pthread_mutex_destroy(&message_mutex);
}

// Thread task acting as a pseudo-server on the west-endpoint of the client
void handle_west_endpoint()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching westbound thread");
        exit(EXIT_FAILURE);
    }

    socklen_t inet_socketfd, other_length;
    struct sockaddr_in inet_addr, otheraddr;

    bzero(&inet_addr, sizeof(inet_addr));
    inet_addr.sin_family  = AF_INET;
    inet_addr.sin_port = htons(listening_port);
    inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // Create network socket
    if((inet_socketfd=socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Error while creating network socket");
        exit(EXIT_FAILURE);
    }

    // Bind the network socket to the given port and any address
    if(bind(inet_socketfd, (const struct sockaddr *)&inet_addr, sizeof(inet_addr)) == -1)
    {
        perror("Error while binding network socket to the port");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming client netowrk requests
    if(listen(inet_socketfd , 5) == -1)
    {
        perror("Error while trying to listen for incoming netowrk clients");
        exit(EXIT_FAILURE);
    }

    while(1) 
    {
        if(connected_with_client)
        {
            sleep(3);
        }
        else
        {        
        	socket_desc_in = accept(inet_socketfd, (struct sockaddr *)&otheraddr, &other_length);
            other_length = sizeof(otheraddr);
            printf("\nWestbound interface has established a conntection with its neighbour.\n");
            connected_with_client = 1;
        }
    }
}

// Thread task acting as a pseudo-client on the east-endpoint of the main client
void handle_east_endpoint()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching eastbound thread");
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in serv_inet_addr;
    bzero((char *)&serv_inet_addr, sizeof(serv_inet_addr));
    serv_inet_addr.sin_family = AF_INET;
    serv_inet_addr.sin_port = htons(neighbour_port);
    serv_inet_addr.sin_addr = neighbour_ip_addr;

    if((socket_desc_out = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Error while creating network socket on the east endpoint");
        exit(EXIT_FAILURE);
    }
    
    while(1)
    {
        if(connected_with_server)
        {
            sleep(3);
        }
        else
        {
            if(!reconnecting)
            {
                if(connect(socket_desc_out, (struct sockaddr *)&serv_inet_addr, sizeof(serv_inet_addr)) == 0) 
                {
                    printf("\nConnection with neightbour's westbound interface successfully established.\n");
                    connected_with_server = 1;
                }
            }
            else
            {
                close(socket_desc_out);
                if((socket_desc_out = socket(AF_INET, SOCK_STREAM, 0)) == -1)
                {
                    perror("Error while re-creating network socket on the east endpoint");
                    exit(EXIT_FAILURE);
                }
                reconnecting = 0;
                has_my_successor_reconnected = 1;
            }
            sleep(1);
        }
    }
}

// Handler for the SIGALRM interrupt
void handler_sigalrm(int signo)
{   
    const char *info = "\nAlarm received!\n";
    
    if(signo == SIGALRM)
    {
        write(1, info, strlen(info));
    }
}

// Handler for the SIGPIPE interrupt
void handler_sigpipe(int signo)
{   
    const char *info = "\nBroken chain detected!\n";
    
    if(signo == SIGPIPE)
    {
        write(1, info, strlen(info));
        connected_with_server = 0;
        reconnecting = 1;
    }
}

// Notifies loggers of receiving a new token
void multicast_to_loggers()
{
    char log_message[LOG_MESSAGE_SIZE];
    bzero(log_message, sizeof(log_message));
    sprintf(log_message, "%s - %lu\n", client_id, (unsigned long)time(NULL)); 
    if(sendto(loggers_socket, log_message, sizeof(log_message), 0, (struct sockaddr *) &loggers_addr, sizeof(loggers_addr)) < 0)
    {
	    perror("Multicast log failed");
	    exit(EXIT_FAILURE);
	}
    sleep(1);
}

// Handle an incoming token
void handle_token()
{
    token token;

    // if your dead successor has reconnted then proceed as if though if had the token
    if(has_my_successor_reconnected) 
    {
        has_token = 1;
        has_my_successor_reconnected = 0;
    }
    
    // if has_token is true then proceed as if you had an empty token
    if(has_token)
    {
        token.type = EMPTY;
        has_token = 0;
        sleep(1);

    }
    // else just block and wait for the token to come from your neighbour
    else
    {
        recv(socket_desc_in, &token, sizeof(token), 0);
    }
    
    // Is the message directed to me?
    if(token.type == FULL && strcmp(token.payload.receiver, client_id) == 0)
    {
        printf("\nReceived message from %s: %s\n", token.payload.sender, token.payload.buffer);
        token.type = RCVD;
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // Disallow inifnitely looping of messages around the ring
    if(token.type == FULL && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // If my message was received
    if(token.type == RCVD && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // if my left neighbour is dying, make notice of that and pass
    if(token.type == CTRL)
    {
        connected_with_client = 0;
        token.type = EMPTY;
        if((write(socket_desc_out, &token, sizeof(token))) < 0) 
        {
            fprintf(stderr, "Error while sending dying message\n");
            return;
        }
    }
    
    // Send msg if necessary and token is empty
    if(should_send_msg && token.type == EMPTY) 
    {
        if(!need_to_skip_turn) 
        {
            multicast_to_loggers();
            if((write(socket_desc_out, &to_send, sizeof(to_send))) < 0) 
            {
                fprintf(stderr, "Error while sending message\n");
                return;
            }
            else
            {
                should_send_msg = 0;
                bzero(&to_send, sizeof(to_send));
                need_to_skip_turn = 1;
                return;
            }
        }
        else
        {
            need_to_skip_turn = 0;
        }
    }
    else if(should_quit && token.type == EMPTY)
    {
        // Inform your neighbour you are going to die
        token.type = CTRL;
        multicast_to_loggers();
        if((write(socket_desc_out, &token, sizeof(token))) < 0) 
        {
            fprintf(stderr, "Error while sending dying message\n");
            return;
        }
        else
        {
            exit(EXIT_SUCCESS);
        }
    }
    
    // otherwise just pass the token to the neighbour
    multicast_to_loggers();
    write(socket_desc_out, &token, sizeof(token));
}

// Thread task acting as a pseudo-client on the east-endpoint of the main client
void handle_east_endpoint_dbc()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching eastbound thread");
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in serv_inet_addr;
    bzero((char *)&serv_inet_addr, sizeof(serv_inet_addr));
    serv_inet_addr.sin_family = AF_INET;
    serv_inet_addr.sin_port = htons(neighbour_port);
    serv_inet_addr.sin_addr = neighbour_ip_addr;

    if((socket_desc_out = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Error while creating network socket on the east endpoint");
        exit(EXIT_FAILURE);
    }
    
    int connected = 0;

    while(1)
    {
        if(!should_change_address && !connected)
        {
            if(connect(socket_desc_out, (struct sockaddr *)&serv_inet_addr, sizeof(serv_inet_addr)) == 0) 
            {
                printf("\nConnected...\n");
                connected = 1;
            }
        }
        else if(should_change_address)
        {
            shutdown(socket_desc_out, SHUT_RDWR);
            close(socket_desc_out);
            bzero((char *)&serv_inet_addr, sizeof(serv_inet_addr));
            serv_inet_addr.sin_family = AF_INET;
            serv_inet_addr.sin_port = htons(neighbour_port_relinked);
            serv_inet_addr.sin_addr = neighbour_ip_addr_relinked;
            if((socket_desc_out = socket(AF_INET, SOCK_STREAM, 0)) == -1)
            {
                perror("Error while re-creating network socket on the east endpoint");
                exit(EXIT_FAILURE);
            }
            should_change_address = 0;
            connected = 0;
        }
        sleep(1);
    }
}

// Thread task acting as a pseudo-server on the west-endpoint of the client
void handle_west_endpoint_dbc()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching westbound thread");
        exit(EXIT_FAILURE);
    }

    socklen_t inet_socketfd, other_length;
    struct sockaddr_in inet_addr, otheraddr;

    bzero(&inet_addr, sizeof(inet_addr));
    inet_addr.sin_family  = AF_INET;
    inet_addr.sin_port = htons(listening_port);
    inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // Create network socket
    if((inet_socketfd=socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Error while creating network socket");
        exit(EXIT_FAILURE);
    }

    // Bind the network socket to the given port and any address
    if(bind(inet_socketfd, (const struct sockaddr *)&inet_addr, sizeof(inet_addr)) == -1)
    {
        perror("Error while binding network socket to the port");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming client netowrk requests
    if(listen(inet_socketfd , 5) == -1)
    {
        perror("Error while trying to listen for incoming netowrk clients");
        exit(EXIT_FAILURE);
    }
    
    while(1) 
    {     
        socket_desc_in = accept(inet_socketfd, (struct sockaddr *)&otheraddr, &other_length);
        printf("Accepted...\n");
        other_length = sizeof(otheraddr);
    }
    
}

// helper function
void tell_predecessor_to_change_output(message relink_info)
{
    bzero(&relink_token, sizeof(relink_token));
    relink_token.type = RELINK;
    relink_token.length = sizeof(token);
    sprintf(relink_token.payload.predecessor, "%s", client_id);
    sprintf(relink_token.payload.receiver, "%s", predecessor_name);
    sprintf(relink_token.payload.sender, "%s", client_id);
    sprintf(relink_token.payload.relinked, "%s", relink_info.relinked);
    should_send_relink_info = 1;
}

// helper function
void relink_output_info(char *new_address)
{
    char ip_addr_port_neighbour_new[32];
    char *token, *ip_addr_human_new;
    strcpy(ip_addr_port_neighbour_new, new_address);
    token = strtok(ip_addr_port_neighbour_new, ":");
    ip_addr_human_new = token;
    token = strtok(NULL, ":");
    neighbour_port_relinked = (in_port_t)strtol(token, NULL, 10);
    printf("\n chaning output address to %s:%d...\n", ip_addr_human_new, neighbour_port_relinked);
    if(inet_aton(ip_addr_human_new, &neighbour_ip_addr_relinked) == 0)
    {
        fprintf(stderr, "Wrong neighbour ip address format!\n");
        exit(EXIT_FAILURE);
    }

}

// Handle an incoming token
void handle_token_dbc()
{
    token token;

    if(!was_hello_send && listening_port != neighbour_port)
    {
        was_hello_send = 1;
        token.type = HELLO;
        token.length = sizeof(token);
        sprintf(token.payload.relinked, "%s:%d", IPbuffer, listening_port);
        write(socket_desc_out, &token, sizeof(token));
        return;
    }
    
    // if has_token is true then proceed as if you had an empty token
    if(has_token)
    {
        token.type = EMPTY;
        has_token = 0;
        sleep(1);

    }
    // else just block and wait for the token to come from your neighbour
    else
    {
        int ret = recv(socket_desc_in, &token, sizeof(token), MSG_WAITALL);
        if (ret == -1)
        {
            return;
        }
        if(token.type != HELLO)
        {
            sprintf(predecessor_name, "%s", token.payload.predecessor);
        }
    }
    
    // Is the message directed to me?
    if(token.type == FULL && strcmp(token.payload.receiver, client_id) == 0)
    {
        printf("\nReceived message from %s: %s\n", token.payload.sender, token.payload.buffer);
        token.type = RCVD;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // Disallow inifnitely looping of messages around the ring
    if(token.type == FULL && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // If my message was received
    if(token.type == RCVD && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // if my successor told me to relink my output then do so
    if(token.type == RELINK && strcmp(token.payload.receiver, client_id) == 0)
    {
        relink_output_info(token.payload.relinked);
        should_change_address = 1;
        sleep(3);
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        write(socket_desc_out, &token, sizeof(token));
        return;
    }

    // if this is a HELLO auxillary token then payload buffer has <IP as ascii> : <port as ascii>
    if(token.type == HELLO)
    {
        // need to tell my predecessor to link their output to the HELLO's sender input
        
        // special case if only one host is in the network
        if(strcmp(predecessor_name, client_id) == 0)
        {
            relink_output_info(token.payload.relinked);
            should_change_address = 1;
            sleep(3);
            sprintf(token.payload.predecessor, "%s", client_id);
            token.type = EMPTY;
            write(socket_desc_out, &token, sizeof(token));
            return;
        }
        else 
        {
            tell_predecessor_to_change_output(token.payload);
            has_token = 1;
            return;
        }
    }

    // If there's a relink msg to be send it has priority over normal msg
    if(should_send_relink_info && token.type == EMPTY) 
    {
        multicast_to_loggers();
        sprintf(relink_token.payload.predecessor, "%s", client_id);
        if((write(socket_desc_out, &relink_token, sizeof(relink_token))) < 0) 
        {
            fprintf(stderr, "Error while sending relink msg\n");
            return;
        }
        else
        {
            should_send_relink_info = 0;
            bzero(&relink_token, sizeof(relink_token));
            return;
        }
    }
    
    // Send msg if necessary and token is empty
    if(should_send_msg && token.type == EMPTY) 
    {
        if(!need_to_skip_turn) 
        {
            multicast_to_loggers();
            sprintf(to_send.payload.predecessor, "%s", client_id);
            if((write(socket_desc_out, &to_send, sizeof(to_send))) < 0) 
            {
                fprintf(stderr, "Error while sending message\n");
                return;
            }
            else
            {
                should_send_msg = 0;
                bzero(&to_send, sizeof(to_send));
                need_to_skip_turn = 1;
                return;
            }
        }
        else
        {
            need_to_skip_turn = 0;
        }
    }
    
    // otherwise just pass the token to the neighbour
    multicast_to_loggers();
    sprintf(token.payload.predecessor, "%s", client_id);
    write(socket_desc_out, &token, sizeof(token));
}

// Pop message from the queue and and to send
void prepare_message()
{
    pthread_mutex_lock(&message_mutex);
    if(should_send_msg == 0 && !is_message_queue_empty(&queue))
    {
        message *popped = pop_message_queue(&queue);
        sprintf(to_send.payload.buffer, "%s", popped->buffer);
        sprintf(to_send.payload.sender, "%s", popped->sender);
        sprintf(to_send.payload.receiver, "%s", popped->receiver);
        to_send.type = FULL;
        to_send.length = sizeof(token);
        free(popped);
        should_send_msg = 1;
    }
    pthread_mutex_unlock(&message_mutex);
}

// Thread task acting as a pseudo-client on the east-endpoint of the main client
void handle_east_endpoint_dbc_udp()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching eastbound thread");
        exit(EXIT_FAILURE);
    }
    
    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(neighbour_port);
    server_addr.sin_addr = neighbour_ip_addr;

    if((socket_desc_out = socket(AF_INET, SOCK_DGRAM, 0)) == -1)
    {
        perror("Error while creating network socket on the east endpoint");
        exit(EXIT_FAILURE);
    }

    while(1)
    {
        if(should_change_address) 
        {
            //close(socket_desc_out);
            bzero((char *)&server_addr, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(neighbour_port_relinked);
            server_addr.sin_addr = neighbour_ip_addr_relinked;
            should_change_address = 0;
        }
        sleep(1);
    }
}

// Thread task acting as a pseudo-server on the west-endpoint of the client
void handle_west_endpoint_dbc_udp()
{
    if(pthread_detach(pthread_self()) != 0)
    {
        fprintf(stderr, "Error while detaching westbound thread");
        exit(EXIT_FAILURE);
    }

    bzero(&client_addr, sizeof(client_addr));
    client_addr.sin_family  = AF_INET;
    client_addr.sin_port = htons(listening_port);
    client_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // Create network socket
    if((socket_desc_in=socket(AF_INET, SOCK_DGRAM, 0)) == -1)
    {
        perror("Error while creating network socket");
        exit(EXIT_FAILURE);
    }

    // Bind the network socket to the given port and any address
    if(bind(socket_desc_in, (const struct sockaddr *)&client_addr, sizeof(client_addr)) == -1)
    {
        perror("Error while binding network socket to the port");
        exit(EXIT_FAILURE);
    }
    
}

// Handle an incoming token
void handle_token_dbc_udp()
{
    token token;

    if(!was_hello_send && listening_port != neighbour_port)
    {
        was_hello_send = 1;
        token.type = HELLO;
        token.length = sizeof(token);
        sprintf(token.payload.relinked, "%s:%d", IPbuffer, listening_port);
        sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr)); 
        return;
    }
    
    // if has_token is true then proceed as if you had an empty token
    if(has_token)
    {
        token.type = EMPTY;
        has_token = 0;
        sleep(1);

    }
    // else just block and wait for the token to come from your neighbour
    else
    {
        int ret = recvfrom(socket_desc_in, &token, sizeof(token), 0, NULL, NULL);
        if (ret == -1)
        {
            return;
        }
        if(token.type != HELLO)
        {
            sprintf(predecessor_name, "%s", token.payload.predecessor);
        }
    }
    
    // Is the message directed to me?
    if(token.type == FULL && strcmp(token.payload.receiver, client_id) == 0)
    {
        printf("\nReceived message from %s: %s\n", token.payload.sender, token.payload.buffer);
        token.type = RCVD;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
        return;
    }

    // Disallow inifnitely looping of messages around the ring
    if(token.type == FULL && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
        return;
    }

    // If my message was received
    if(token.type == RCVD && strcmp(token.payload.sender, client_id) == 0)
    {
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
        return;
    }

    // if my successor told me to relink my output then do so
    if(token.type == RELINK && strcmp(token.payload.receiver, client_id) == 0)
    {
        relink_output_info(token.payload.relinked);
        should_change_address = 1;
        sleep(3);
        token.type = EMPTY;
        sprintf(token.payload.predecessor, "%s", client_id);
        multicast_to_loggers();
        sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
        return;
    }

    // if this is a HELLO auxillary token then payload buffer has <IP as ascii> : <port as ascii>
    if(token.type == HELLO)
    {
        // need to tell my predecessor to link their output to the HELLO's sender input
        
        // special case if only one host is in the network
        if(strcmp(predecessor_name, client_id) == 0)
        {
            relink_output_info(token.payload.relinked);
            should_change_address = 1;
            sleep(3);
            sprintf(token.payload.predecessor, "%s", client_id);
            token.type = EMPTY;
            sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
            return;
        }
        else 
        {
            tell_predecessor_to_change_output(token.payload);
            return;
        }
    }

    // If there's a relink msg to be send it has priority over normal msg
    if(should_send_relink_info && token.type == EMPTY) 
    {
        multicast_to_loggers();
        sprintf(relink_token.payload.predecessor, "%s", client_id);
        if((sendto(socket_desc_out, &relink_token, sizeof(relink_token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr))) < 0) 
        {
            fprintf(stderr, "Error while sending relink msg\n");
            return;
        }
        else
        {
            should_send_relink_info = 0;
            bzero(&relink_token, sizeof(relink_token));
            return;
        }
    }
    
    // Send msg if necessary and token is empty
    if(should_send_msg && token.type == EMPTY) 
    {
        if(!need_to_skip_turn) 
        {
            multicast_to_loggers();
            sprintf(to_send.payload.predecessor, "%s", client_id);
            if((sendto(socket_desc_out, &to_send, sizeof(to_send), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr))) < 0) 
            {
                fprintf(stderr, "Error while sending message\n");
                return;
            }
            else
            {
                should_send_msg = 0;
                bzero(&to_send, sizeof(to_send));
                need_to_skip_turn = 1;
                return;
            }
        }
        else
        {
            need_to_skip_turn = 0;
        }
    }
    
    // otherwise just pass the token to the neighbour
    multicast_to_loggers();
    sprintf(token.payload.predecessor, "%s", client_id);
    sendto(socket_desc_out, &token, sizeof(token), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
}

// MAIN ----------------------------------------------------------------------
int main(int argc, char** argv) 
{
    char ip_addr_port_neighbour[32];

    if (argc < 6) sig_arg_err();

    client_id = argv[1];
    if(strlen(client_id) > MAX_CLIENT_ID)
    {
        fprintf(stderr, "The provided client id is too long!\n");
        exit(EXIT_FAILURE);
    }

    listening_port = (in_port_t)atoi(argv[2]);

    strcpy(ip_addr_port_neighbour, argv[3]);
    char *token;
    token = strtok(ip_addr_port_neighbour, ":");
    ip_addr_human_neighbour = token;
    token = strtok(NULL, ":");
    neighbour_port = (in_port_t)strtol(token, NULL, 10);
    if(inet_aton(ip_addr_human_neighbour, &neighbour_ip_addr) == 0)
    {
        fprintf(stderr, "Wrong neighbour ip address format!\n");
        exit(EXIT_FAILURE);
    }
    
    if(strcmp(argv[4], "TOKEN") == 0)
        has_token = 1;
    else if(strcmp(argv[4], "NOTOKEN") == 0)
        has_token = 0;
    else
    {
        fprintf(stderr, "Wrong token specification argument format!\n");
        exit(EXIT_FAILURE);
    }
    
    if(strcmp(argv[5], "TCP") == 0)
        connection = TCP;
    else if(strcmp(argv[5], "UDP") == 0)
        connection = UDP;
    else
    {
        fprintf(stderr, "Wrong connection type argument format!\n");
        exit(EXIT_FAILURE);
    }

    // Allow broken chain? If yes then chain can be broken but no dynamic host adding
    // (only when someone leaves), if no then dynamic host adding is possible
    char response;
    printf("Allow broken chain? If yes then clients can disconnect and reconnect but no new hosts can join - (y/n) ");
    do
    {
        scanf("%c", &response);
    } while (response != 'y' && response != 'n');
    if(response == 'y')
        allow_broken_chain = 1;
    else
        allow_broken_chain = 0;

    // Register cleanup
    atexit(perform_cleanup);

    init_message_queue(&queue);

    struct hostent *host_entry;
    char hostbuffer[256]; 
    int hostname;
    hostname = gethostname(hostbuffer, sizeof(hostbuffer)); 
    checkHostName(hostname); 
    host_entry = gethostbyname(hostbuffer); 
    checkHostEntry(host_entry); 
    IPbuffer = inet_ntoa(*((struct in_addr*)host_entry->h_addr_list[0]));

    // Prepare loggers addres
    if((loggers_socket=socket(AF_INET,SOCK_DGRAM,0)) < 0) 
    {
	    perror("Loggers socket failed");
	    exit(EXIT_FAILURE);
    }
    memset(&loggers_addr,0,sizeof(loggers_addr));
    loggers_addr.sin_family=AF_INET;
    loggers_addr.sin_addr.s_addr=inet_addr(LOGGERS_GROUP);
    loggers_addr.sin_port=htons(LOGGERS_PORT);

    if(connection == TCP) 
    {
        if(allow_broken_chain) 
        {
            
            if(signal(SIGINT, handler_sigint) == SIG_ERR)
            {
                perror("Error while setting the SIGINT handler");
                exit(EXIT_FAILURE);
            }
            if(signal(SIGALRM, handler_sigalrm) == SIG_ERR)
            {
                perror("Error while setting the SIGALRM handler");
                exit(EXIT_FAILURE);
            }
            if(signal(SIGPIPE, handler_sigpipe) == SIG_ERR)
            {
                perror("Error while setting the SIGPIPE handler");
                exit(EXIT_FAILURE);
            }
            
            pthread_create(&server_thread, NULL, (void *(*)(void *))handle_west_endpoint, NULL);
            pthread_create(&client_thread, NULL, (void *(*)(void *))handle_east_endpoint, NULL);
            pthread_create(&input_handle_thread, NULL, input_handle_task, NULL);
        
            while(1)
            {
                if(!connected_with_client || !connected_with_server) 
                {
                    sleep(1);
                    continue;
                }
                handle_token();
                prepare_message();
            }
        } 
        else
        {
            sprintf(predecessor_name, "%s", client_id);
            
            pthread_create(&input_handle_thread, NULL, input_handle_task, NULL);
            pthread_create(&server_thread, NULL, (void *(*)(void *))handle_west_endpoint_dbc, NULL);
            pthread_create(&client_thread, NULL, (void *(*)(void *))handle_east_endpoint_dbc, NULL);

            sleep(1);

            while(1)
            {
                handle_token_dbc();
                prepare_message();
            }
        }
    } 
    else if(connection == UDP)
    {
        sprintf(predecessor_name, "%s", client_id);
            
        pthread_create(&input_handle_thread, NULL, input_handle_task, NULL);
        pthread_create(&server_thread, NULL, (void *(*)(void *))handle_west_endpoint_dbc_udp, NULL);
        pthread_create(&client_thread, NULL, (void *(*)(void *))handle_east_endpoint_dbc_udp, NULL);

        sleep(1);

        while(1)
        {
            handle_token_dbc_udp();
            prepare_message();
        }
    }

    pthread_join(server_thread, NULL);
    pthread_join(client_thread, NULL);
    pthread_join(input_handle_thread, NULL);
        
    return 0;
}