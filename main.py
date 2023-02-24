import datetime
import os,shutil
from my_function.preparation import QSCORE, BestQSCORE



qscore = QSCORE()
bqscore = BestQSCORE()

if __name__=="__main__":
        try:
            qscore.update_to_qscordb()
            print('Qscore Update successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Qscore Update error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
        try:
            bqscore.update_to_bestqscordb()
            print('Best Qscore Update successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Best Qscore Update error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            