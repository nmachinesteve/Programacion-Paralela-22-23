from multiprocessing.connection import Client
import traceback
import pygame
import sys, os
import time
import random

# Puntuación para ganar 
PMAX = 10

# Tiempo que dura la partida
TMAX = 600

BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
RED = (255, 0, 0)
BLUE = (0, 0, 255)
YELLOW = (255,255,0)
GREEN = (0,255,0)
X = 0
Y = 1
SIZE = (1000,612)

PLAYER_0 = 0
PLAYER_1 = 1
TURN = ["one","two"]

# Las imagenes que vamos a usar para los jugadores
PlAYER_IMAGE = ["player0.png","player1.png"]
PLAYER_HEIGHT = 10
PLAYER_WIDTH = 60

# Cosas de los globos
NMAX = 10 #Numero de globos 

# Imagenes de los globos
# Creamos un diccionario con las imagenes de los globos y luego el código lo 
# elige de manera aleatoria.
BIMAGES = {"balloon.png":(150,150),"balloon3.png":(60,90),"balloons.png":(150,150)}

class Player():
    def __init__(self, turn):
        self.turn = turn
        self.pos = [None, None] 

    def get_pos(self):
        return self.pos
  
    def get_turn(self):
        return self.turn

    def set_pos(self, pos):
        self.pos = pos

    def __str__(self):
        return f"P<{TURN[self.turn], self.pos}>"

class Balloon():
    def __init__(self,index):
        self.pos=[None, None]
        self.index = index

    def get_pos(self):
        return self.pos

    def set_pos(self, pos):
        self.pos = pos
    
    
    def __str__(self):
        return f"B<{self.index, self.pos}>"

class Game():
    def __init__(self):
        self.players = [Player(i) for i in range(2)]
        self.balloons = [Balloon(i) for i in range(NMAX)]
        self.score = [0,0]
        self.running = True
        self.time = time.time() 
        self.end = False

    def get_player(self, turn):
        return self.players[turn]

    def set_pos_player(self, turn, pos):
        self.players[turn].set_pos(pos)

    def get_balloon(self,index): 
        return self.balloons[index]
        
    def set_balloon_pos(self, index,pos):
        self.balloons[index].set_pos(pos)

    def get_score(self):
        return self.score

    def set_score(self, score):
        self.score = score
        
    def get_time(self):  
        return self.time
    
    def set_time(self,time):
        self.time=time

    def update(self, gameinfo):
        self.set_pos_player(PLAYER_0, gameinfo['pos_left_player'])
        self.set_pos_player(PLAYER_1, gameinfo['pos_right_player'])
        info_balloons = gameinfo['pos_balloons']
        for i in range(NMAX):
            balloon_i = info_balloons[i]
            self.set_balloon_pos(i, balloon_i)
        self.set_score(gameinfo['score'])
        self.running = gameinfo['is_running']
        self.end = gameinfo['ended']

    def is_running(self):
        return self.running

    def stop(self):
        self.running = False

    def __str__(self):
        return f"G<{self.players[PLAYER_1]}:{self.players[PLAYER_0]}:{self.balloons}>"

class Paddle(pygame.sprite.Sprite):
    def __init__(self, player):
      super().__init__()
      self.player = player
      if self.player.turn == 0:
          self.image = pygame.image.load("player0.png")
          self.image = pygame.transform.scale(self.image,(140,180))
          self.rect = self.image.get_rect()
          self.update()
      else:
          self.image = pygame.image.load("player1.png")
          self.image = pygame.transform.scale(self.image,(125,180))
          self.rect = self.image.get_rect()
          self.update()
      
    def update(self):
        pos = self.player.get_pos()
        self.rect.centerx, self.rect.centery = pos
        
    def draw(self,screen):
        screen.window.blit(self.image, self.rect)

    def __str__(self):
        return f"S<{self.player}>"

class BallSprite(pygame.sprite.Sprite):
    def __init__(self, balloon):
        super().__init__()
        self.balloon = balloon
        image = random.choice(list(BIMAGES.keys()))
        self.image = pygame.image.load(image)
        self.image = pygame.transform.scale(self.image,BIMAGES[image])
        self.image.set_colorkey(WHITE)
        self.rect = self.image.get_rect()
        self.update()

    def update(self):
        pos = self.balloon.get_pos()
        self.rect.centerx, self.rect.centery = pos
    
    def draw(self,screen):
        screen.window.blit(self.image,(self.balloon.pos))
               
class Display():
    def __init__(self, game):
        self.game = game
        self.paddles = [Paddle(self.game.get_player(i)) for i in range(2)]
        self.balloons = [BallSprite(self.game.get_balloon(i)) for i in range(NMAX)]
        self.time=game.get_time()
        self.all_sprites = pygame.sprite.Group()
        self.paddle_group = pygame.sprite.Group()
        self.balloon_group = pygame.sprite.Group()
        for paddle  in self.paddles:
            self.all_sprites.add(paddle)
            self.paddle_group.add(paddle)  
        for balloon in self.balloons:
            self.all_sprites.add(balloon)
            self.balloon_group.add(balloon)
        self.screen = pygame.display.set_mode(SIZE) 
        self.background = pygame.image.load('parke.png')
        pygame.init()

    def analyze_events(self, turn):
        events = []
        for event in pygame.event.get():
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    events.append("quit")
                elif event.key == pygame.K_LEFT:
                    events.append("left")
                elif event.key == pygame.K_RIGHT:
                    events.append("right")
                elif event.key == pygame.K_SPACE:
                    events.append("space")
            elif event.type == pygame.QUIT:
                events.append("quit")
        return events

    def refresh(self,end,score,turn):
        """
        end es un booleano, se inicializa en False, se vuelve True cuando se 
        ha acabado el tiempo o algún jugador ha llegado al número de globos 
        sufcientes. 
        Cuando end es True llamamos a la función self.show_end
        """
        if end:
            self.show_end(score,turn)
        else:
            self.all_sprites.update()
            self.screen.blit(self.background, (0, 0))
            score = self.game.get_score()
            time = self.game.get_time()
            font = pygame.font.Font(None, 74)
            text = font.render(f"{score[PLAYER_0]}", 1, WHITE)
            self.screen.blit(text, (250, 10))
            text = font.render(f"{score[PLAYER_1]}", 1, WHITE)
            self.screen.blit(text, (SIZE[X]-250, 10))
            self.all_sprites.draw(self.screen)
            pygame.display.flip()
            
    def show_end(self,score,turn):
        """
        Devuelve la pantalla solo con el fondo y un mensaje en función de la 
        puntuación.
        """
        j1 = score[0]
        j2 = score[1]
        if j1 != j2:
            if score[turn] == max(j1,j2):
                r = "¡HA GANADO! "
            else:
                r = "¡HA PERDIDO! "
        else:
            r = "EMPATE"
        self.screen.blit(self.background, (0, 0))
        font = pygame.font.Font(None, 200)
        text = font.render(f"{r}", 1, BLUE)
        self.screen.blit(text, (0, 412))
        pygame.display.flip()
        
    @staticmethod
    def quit():
        pygame.quit()

def main(ip_address):
    try:
        with Client((ip_address, 6000), authkey=b'secret password') as conn:
            game = Game()
            turn,gameinfo = conn.recv()
            print(f"I am playing {TURN[turn]}")
            game.update(gameinfo)
            display = Display(game)
            print('running: ', game.is_running())
            while game.is_running():
                events = display.analyze_events(turn)
                for ev in events:
                    conn.send(ev)
                    if ev == 'quit':
                        game.stop()
                conn.send("next")
                gameinfo = conn.recv()
                game.update(gameinfo)
                display.refresh(game.end,game.score,turn)

    except:
        traceback.print_exc()
    finally:
        pygame.quit()

if __name__=="__main__":
    ip_address = "127.0.0.1"
    if len(sys.argv)>1:
        ip_address = sys.argv[1]
    main(ip_address)
