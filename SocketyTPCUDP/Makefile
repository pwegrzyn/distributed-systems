CC := gcc
CFLAGS := -Wall -std=c99 -g
LDLIBS := -lpthread

all: clean client logger

client: client_tokenring.o
	$(CC) $(CFLAGS) -o client_tokenring client_tokenring.o $(LDLIBS)

client.o: client_tokenring.c
	$(CC) $(CFLAGS) -c client_tokenring.c

logger:
	javac *.java

clean: 
	rm -f *.o client_tokenring *.class