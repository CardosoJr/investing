import pandas as pd 

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime

class Gdrive:
    def __init__(self, credential_path):
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(credential_path)
        # gauth.LocalWebserverAuth() # client_secrets.json need to be in the same directory as the script
        self.drive = GoogleDrive(gauth)

    def read_csv(self, file_id):
        fileDownloaded = self.drive.CreateFile({'id' : "1Vl0p4F4dkcKJZMx1Bmyc1fPpRbx7s8mv"})
        temp_file_name = "f_{0}.csv".format(datetime.now().strftime("%y%m%d%H%M%f"))
        fileDownloaded.GetContentFile(temp_file_name)
        return pd.read_csv(temp_file_name)

    def save_csv(self, parent_id, df):
        file1 = self.drive.CreateFile({"mimeType": "text/csv", "parents": [{"kind": "drive#fileLink", "id": parent_id}]})
        temp_file_name = "f_{0}.csv".format(datetime.now().strftime("%y%m%d%H%M%f"))
        df.to_csv(temp_file_name)
        file1.SetContentFile(temp_file_name)
        file1.Upload() # Upload the file.

    def update_csv(self, file_id, df):
        file1 = self.drive.CreateFile({"id" : file_id})
        temp_file_name = "f_{0}.csv".format(datetime.now().strftime("%y%m%d%H%M%f"))
        df.to_csv(temp_file_name)
        file1.SetContentFile(temp_file_name)
        file1.Upload() # Upload the file.

    def get_children(self, root_folder_id):
        str = "\'" + root_folder_id + "\'" + " in parents and trashed=false"
        file_list = self.drive.ListFile({'q': str}).GetList()
        return file_list

    def get_folder_id(self, root_folder_id, root_folder_title):
        file_list = self.get_children(root_folder_id)
        for file in file_list:
            if(file['title'] == root_folder_title):
                return file['id']

    def get_folder_id(self):
        pass

    def get_file_id(self):
        pass


# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive

# gauth = GoogleAuth()
# gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication

# #Make GoogleDrive instance with Authenticated GoogleAuth instance
# drive = GoogleDrive(gauth)

# def ListFolder(parent):
#   filelist=[]
#   file_list = drive.ListFile({'q': "'%s' in parents and trashed=false" % parent}).GetList()
#   for f in file_list:
#     if f['mimeType']=='application/vnd.google-apps.folder': # if folder
#         filelist.append({"id":f['id'],"title":f['title'],"list":ListFolder(f['id'])})
#     else:
#         filelist.append({"title":f['title'],"title1":f['alternateLink']})
#   return filelist

# ListFolder('root')