import pandas as pd
import os, shutil
import datetime
import warnings

warnings.filterwarnings("ignore")

from my_db.db import QSCOREDB
db = QSCOREDB()

class QA32():
    def __init__(self) -> None:
        pass
    
    def filters_qa32(self,input_data):
        try:
            data = input_data.copy()
            ##filter
            data = data.loc[~data['System Status'].str.startswith(('LTCA'))]
            data = data[(data['Inspection Type'] == 'ZY2')]
            data = data[ (data['Plant'] != 'E001')]
            return data
        except:
            print('filters_qa32 error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def col_rename_qa32(self,input_data):
        try:
            data = input_data.copy()
            data = data.rename(columns={'Batch':'batch',
                                        'Material':'material',
                                        'Plant':'plant',
                                        'Quality Score':'qscore',
                                        'Lot Created On':'date'
            })
            return data
        except:
            print('col_rename_qa32 error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def col_sel_qa32(self,input_data):
        try:    
            ## columns selection
            data = input_data.copy()
            col_list = ['batch','material','plant','qscore','date']
            data = data[col_list]
            return data
        except:
            print('col_sel_qa32 error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def transform_qa32(self,input_data):
        try:
            filters_qa32 = self.filters_qa32(input_data)
            col_rename_qa32 = self.col_rename_qa32(filters_qa32)
            col_sel_qa32 = self.col_sel_qa32(col_rename_qa32)
            data = col_sel_qa32
            return data
        except:
            print('transform_qa32 error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def processing_qa32(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rawdata/qa32_rawdata/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                combined_data = pd.DataFrame()
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    qa32_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    qa32 = self.transform_qa32(qa32_df)
                    combined_data = combined_data.append(qa32,ignore_index=True)
                    print('\t%s' % fname,':transform Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                # combined_data.to_excel("./documents/qscore/"+'Summary_qa32.xlsx')
                # print('qa32 transform Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                return combined_data
        except:
            print('qa32 transform error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
class FEEDM():
    def __init__(self) -> None:
        pass
      
    def filters_feedm(self,input_data):
        try:
            data = input_data.copy()
            data = data.fillna(0)
            data = data.loc[data['Material'].str.startswith(("P", "V", "C"))]
            data = data.loc[~data['Queue Registered Plant'].str.startswith(('E001'))]

            data = data.rename(columns={'Batch':'batch',
                                                        'Material':'material',
                                                        'Queue Registered Plant':'plant',
                                                        'Vendor':'vendor',
                                                        'Queue Date':'date'
                                                        })

            col_list = ['batch','material','plant','vendor','date']
            data = data[col_list]
            return data
        except:
            print('filters_feedm transform error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')

    def processing_feedm(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rawdata/feedmill_rawdata'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                combined_data = pd.DataFrame()
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    feedm_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    feedm = self.filters_feedm(feedm_df)
                    combined_data = combined_data.append(feedm,ignore_index=True)
                    print('\t%s' % fname,':transform Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                # combined_data.to_excel("./documents/qscore/feedmill/"+'Summary_FeedMill_newversion.xlsx')
                # print('processing_feedm Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                return combined_data
        except:
            print('processing_feedm error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
class NOTI():
    def __init__(self) -> None:
        pass
    
    def filters_noti(self,input_data):
        try:
            data = input_data.copy()
            ##filter
            data = data[data['Notification type'] == "ZC"]
            data = data[~data['Status of NC'].str.contains('DLFL', na=False)]
            data = data.loc[data['Plant for inspection lot'].str.startswith(('A'))]
            return data
        except:
            print('filters_noti error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def col_rename_noti(self,input_data):
        try:
            data = input_data.copy()
            data = data.rename(columns={'Batch no':'batch',
                                'Product code':'material',
                                'Plant for inspection lot':'plant',
                                'Supplier code':'vendor',
                                'Date of notification':'date',
                                'Notification no':'Notification'
                                })
            return data
        except:
            print('col_rename_noti error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def col_sel_noti(self,input_data):
        try:    
            ## columns selection
            data = input_data.copy()
            data['nscore'] = 99
            col_list = ['batch','material','plant','vendor','date','Notification','nscore']
            data = data[col_list]
            return data
        except:
            print('col_sel_noti error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def transform_noti(self,input_data):
        try:
            filters_noti = self.filters_noti(input_data)
            col_rename_noti = self.col_rename_noti(filters_noti)
            col_sel_noti = self.col_sel_noti(col_rename_noti)
            data = col_sel_noti
            return data
        except:
            print('transform_noti error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def processing_noti(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rawdata/notification_rawdata/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                combined_data = pd.DataFrame()
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    feedm_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    feedm = self.transform_noti(feedm_df)
                    combined_data = combined_data.append(feedm,ignore_index=True)
                    print('\t%s' % fname,':transform Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')   
                # combined_data.to_excel("./documents/qscore/"+'Summary_notification.xlsx')
                # shutil.move('./documents/rawdata/notification_rawdata/','./documents/notification_complete/feedmill_complete/')
                # print('notification transform Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                return combined_data
        except:
            print('notification transform error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
class QSCORE():
    def __init__(self) -> None:
        self.qa32 = QA32()
        self.feedm = FEEDM()
        self.noti = NOTI()
        
    def merger(self):
        try:
            qa32 = self.qa32.processing_qa32()
            qa32 = qa32.set_index(['batch','material','plant'])
            feedm = self.feedm.processing_feedm()
            feedm = feedm.set_index(['batch','material','plant'])
            # feedm = feedm.drop(columns={'Unnamed: 0'})
            noti = self.noti.processing_noti()
            noti = noti.set_index(['batch','material','plant'])
            
            ### SAP FeedMill merge QA32
            feedm_qa32 = pd.merge(feedm,qa32, how='inner' ,on=['batch','material','plant'])
            feedm_qa32 = feedm_qa32.drop(columns={'date_y'})
            feedm_qa32 = feedm_qa32.rename(columns={'date_x':'date'})
            
            ### SAP FeedMill merge QA32 merge Notifications
            data = pd.merge(feedm_qa32,noti, how='left' ,on=['batch','material','plant'])
            data = data.drop(columns={'vendor_y','date_y','Notification'})
            data = data.rename(columns={'vendor_x':'vendor','date_x':'date'}).reset_index()
            data = data.fillna(0)
            return data
        except:
            print('merger error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def qscore_calculate(self):
        try:
            data = self.merger()
            data['q_score'] = data['qscore'].astype(float) - data['nscore'].astype(float)
            data['year'] = data['date'].dt.year
            data['month'] = data['date'].dt.month
            data = data.drop(columns={'qscore','nscore','plant','batch','date'})
            data[['material','vendor']] = data[['material','vendor']].astype(pd.StringDtype())
            data['vendor'] = data['vendor'].str[:9]
            data = data.groupby(['vendor','material','year','month']).mean().reset_index()
            data['MA'] = 0
            return data
        except:
            print('qscore_calculate error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def checkexit(self):
        try:
            ## new data
            new_qscore = self.qscore_calculate()
            # ## old data
            qscore = db.get_qscore_tbl()
            qscore = qscore.drop(columns={'ID'})
            # ##Compare Data
            data = pd.concat([new_qscore,qscore]).drop_duplicates(keep=False)
            return data
        except:
            print('checkexit error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')        
        
    def update_to_qscordb(self):
        try:
            qscore = self.checkexit()
            for i in range(len(qscore)):
                qscores = qscore.values.tolist()
                qscores = qscores[i][0],qscores[i][1],qscores[i][2],qscores[i][3],qscores[i][4],qscores[i][5]
                db.insert_qscore_tbl(qscores)
        except:
            print('Update qscore error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
class BestQSCORE():
    def __init__(self) -> None:
        pass
         
    def best_qscore(self):
        try:
            data = db.get_qscore_tbl()
            data['MA2'] = data.groupby(['vendor','material'])['q_score'].transform(lambda x: x.rolling(4).mean())
            data = data.drop(columns={'MA'})
            data = data.rename(columns={'MA2':'MA'}) 
            data['yearmonth'] = data['year'].astype(pd.StringDtype()) + data['month'].astype(pd.StringDtype())

            data['yearmonth'] = data['yearmonth'].astype(int)
            data = data.loc[data.groupby(['vendor','material'])['yearmonth'].idxmax()]
            data = data[data['MA'] == 100]
            data = data.drop(columns={'ID','yearmonth'})
            return data
        except:
            print('best_qscore error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def checkexit(self):
        try:
            ## new data
            new_qscore = self.best_qscore()
            ## old data
            qscore = db.get_bestqscore_tbl()
            qscore = qscore.drop(columns={'ID'})
            ##Compare Data
            data = pd.concat([new_qscore,qscore]).drop_duplicates(keep=False)
            return data
        except:
            print('checkexit error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def update_to_bestqscordb(self):
        try:
            bqscore = self.checkexit()
            for i in range(len(bqscore)):
                bqscores = bqscore.values.tolist()
                bqscores = bqscores[i][0],bqscores[i][1],bqscores[i][2],bqscores[i][3],bqscores[i][4],bqscores[i][5]
                db.insert_bestqscore_tbl(bqscores)
                
            data = db.get_bestqscore_tbl()
            data = data.drop_duplicates()
            data['yearmonth'] = data['year'].astype(pd.StringDtype()) + data['month'].astype(pd.StringDtype())
            data['yearmonth'] = data['yearmonth'].astype(int)
            data = data.loc[data.groupby(['vendor','material'])['yearmonth'].idxmax()]
            filename = f"QScore{datetime.datetime.now().strftime('%Y%m')}_Evaluat_{datetime.datetime.now().strftime('%Y%m%d')}"
            data.to_excel("D:/Betagro Public Company Limited/QC_AGRO_Data_Report - Master Data/"+filename+".xlsx")
        except:
            print('Update bestqscor error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            