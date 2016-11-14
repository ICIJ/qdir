OBJECTS = qdir.o

default: qdir
	@echo "Building default..."

%.o: src/main.c
	gcc -c -o $@ $< `pkg-config --cflags hiredis`

qdir: $(OBJECTS)
	gcc -o $@ $^ `pkg-config --cflags --libs hiredis`

clean:
	rm -f qdir $(OBJECTS)

.PHONY: clean
