import datetime
from my_function.preparation import QSCORE

qscore = QSCORE()

if __name__=="__main__":
        try:
            qscore.update_to_db()
            print('Qscore Update successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Qscore Update error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            