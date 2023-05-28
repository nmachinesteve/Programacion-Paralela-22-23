from multiprocessing.connection import Listener
from multiprocessing import Process, Manager, Value, Lock,set_start_method
import traceback
import sys, random
import time 

# JUGADORES
PLAYER_0 = 0
PLAYER_1 = 1
TURN = ["one","two"]

# Puntuación necesaria para ganar 
PMAX = 10

#Tiempo maximo de juego en cada partida
TMAX = 600

#Tamaño de la pantalla 
SIZE = (1000,612) 

# Ejes 
X=0
Y=1

DELTA = 25 #El paso que se da al ir a un lado 
 
NMAX = 10 #num de globos en la pantalla

class Player():
    def __init__(self, turn):
        self.turn = turn
        # Definimos la posición en función del jugador
        if turn == PLAYER_0:
            self.pos = [SIZE[X]//2-50,500]
        else:
            self.pos = [SIZE[X]//2+50,525]
    
    def get_pos(self):
        return self.pos
    
    def get_turn(self):
        return self.turn
            
    def moveRight(self):
        self.pos[X] += DELTA
        if self.pos[X] > SIZE[X]:
            self.pos[X] = SIZE[X]
            
    def moveLeft(self):
        self.pos[X] -= DELTA
        if self.pos[X] < 0:
            self.pos[X] = 0    
         
    def __str__(self):
        return f"P<{TURN[self.turn]}, {self.pos}>"

class Balloon():
    def __init__(self, index):
        """
        Pone el globo en la parte de abajo y en una coordenada X aleatoria
        Velocidad de cada globo aleatoria en el eje vertical 
        """
        self.pos=[random.randint(0, SIZE[X]), 610]
        self.velocity = [0,-random.randint(1,5)] 
        self.index = index

    def get_pos(self):
        return self.pos
    
    def get_index(self):
        return self.index

    def update(self):
        self.pos[X] += self.velocity[X]
        self.pos[Y] += self.velocity[Y]
    
    def moveDisp1(self):
        # Cuando eliminamos un globo este vuelve a aparecer en otra posición
        self.pos = [random.randint(0, SIZE[X]), 610]


    def __str__(self):
        return f"B<{self.index, self.pos, self.velocity}>"


class Game():   
    def __init__(self, manager):
        self.time = time.time() 
        self.players = manager.list( [Player(PLAYER_0), Player(PLAYER_1)] )
        self.balloons = manager.list( [Balloon(i) for i in range(NMAX)])
        self.score = manager.list( [0,0] )
        self.running = Value('i', 1) # 1 running
        self.end = Value('i', 0)
        self.lock = Lock()
        self.mutex = Lock()
 
        
    def get_player(self, turn):
        return self.players[turn]
        
    def get_ballon(self):
        for i in range(NMAX):
            return self.ballons[i]

    def get_score(self):
        return list(self.score)

    def is_running(self):
        return self.running.value == 1
    
    def stop(self):
        """
        Hace que acabe la partida cuando algún jugador explote al menos PMAX 
        globos o se acabe el tiempo.
        """
        if self.score[0] >= PMAX or self.score[1] >= PMAX or time.time() - self.time > TMAX :
            self.end.value = 1
            
    def finish(self):
        self.running.value = 0

    def moveLeft(self, player):
        self.mutex.acquire()
        p = self.players[player]
        p.moveLeft()
        self.players[player] = p
        self.mutex.release()

    def moveRight(self, player):
        self.mutex.acquire()
        p = self.players[player]
        p.moveRight()
        self.players[player] = p
        self.mutex.release()
        
    def moveDisp(self, player):
        """
        Dado un jugador busca si está en el intervalo de la coordenada X 
        de algún globo. Si hay algún globo llama a la función moveDisp1 de la 
        clase Balloon.
        """
        self.mutex.acquire()
        p = self.players[player]
        xp = p.pos[X]
        lista = self.balloons 
        for i in range(NMAX):
            balloon = lista[i]
            e = 13
            if balloon.pos[X]-e < xp and balloon.pos[X]+e > xp:
                b = balloon 
                b.moveDisp1()
                self.score[player] += 1  
                self.balloons[i] = b
        self.mutex.release()

    def get_info(self):
        pos_balloons = []
        for i in range(NMAX):
            pos_balloons.append(self.balloons[i].get_pos())
        info = {
            'pos_left_player': self.players[PLAYER_0].get_pos(),
            'pos_right_player': self.players[PLAYER_1].get_pos(),
            'pos_balloons': pos_balloons,
            'score': list(self.score),
            'is_running': self.running.value == 1,
            'ended': self.end.value == 1
        }
        return info

    def move_ball(self):
        self.lock.acquire()
        for i in range(NMAX):
            balloon = self.balloons[i]
            balloon.update()
            pos = balloon.get_pos()
            if pos[Y]<0 or pos[Y]>SIZE[Y]:
                balloon.pos[Y] = 610
            if pos[X]>SIZE[X]:
                self.score[PLAYER_0] += 1
            elif pos[X]<0:
                self.score[PLAYER_1] += 1
            self.balloons[i]=balloon
        self.lock.release()


    def __str__(self):
        return f"G<{self.players[PLAYER_1]}:{self.players[PLAYER_0]}:{self.balloons}:{self.running.value}>"

def player(turn, conn, game):
    try:
        print(f"starting player {TURN[turn]}:{game.get_info()}")
        conn.send( (turn, game.get_info()) )
        while game.is_running():
            command = ""
            while command != "next":
                command = conn.recv()
                if command == "space":
                    game.moveDisp(turn)
                elif command == "left":
                    game.moveLeft(turn)
                elif command == "right":
                    game.moveRight(turn)
                elif command == "quit":
                   game.finish()
            if turn == 1:
                game.move_ball()
                if game.stop():
                    return f"GAME OVER"
            conn.send(game.get_info())
    except:
        traceback.print_exc()
        conn.close()
    finally:
        print(f"Game ended {game}")

def main(ip_address):
    set_start_method('fork') # Esto es lo que hace que funcione en Mac
    manager = Manager()
    try:
        with Listener((ip_address, 6000),
                      authkey=b'secret password') as listener:
            n_player = 0
            players = [None, None]
            game = Game(manager)
            while True:
                print(f"accepting connection {n_player}")
                conn = listener.accept()
                players[n_player] = Process(target=player,
                                            args=(n_player, conn, game))
                n_player += 1
                if n_player == 2:
                    players[0].start()
                    players[1].start()
                    n_player = 0
                    players = [None, None]
                    game = Game(manager)

    except Exception as e:
        traceback.print_exc()

if __name__=='__main__':
    ip_address = "127.0.0.1"
    if len(sys.argv)>1:
        ip_address = sys.argv[1]

    main(ip_address)
