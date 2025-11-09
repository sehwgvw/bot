# -*- coding: utf-8 -*-
import sqlite3
import config

def newUserRegister(user_id, user_regdata):
    if getUserFromDatabase(user_id) == False:
        conn, cursor = getCursorBatabase()
        cursor.execute("INSERT INTO users VALUES(?, ?, ?, ?);", (user_id, user_regdata, 'menu', ''))
        conn.commit()
        return "Registered"
    else:
        return "False"


def getAllUserFromDatabase():
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM users")
    exists = cursor.fetchall()
    conn.commit()
    if not exists:
        return False
    else:
        return exists


def getUserFromDatabase(user_id):
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM users WHERE user_id=?",(user_id,))
    exists = cursor.fetchall()
    conn.commit()
    if not exists:
        return False
    else:
        return exists[0]

def setUserMenu(user_id, menu):
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM users WHERE user_id=?",(user_id,))
    exists = cursor.fetchall()
    if exists:
        cursor.execute("UPDATE users SET user_menu=? WHERE user_id=?", (menu, user_id,))
    conn.commit()

def getUserMenu(user_id):
    return getUserFromDatabase(user_id)[2]



def getAllFilesFromDatabase():
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM files")
    exists = cursor.fetchall()
    conn.commit()
    if not exists:
        return False
    else:
        return exists


def getUserFilesList(user_id):
    files_list = getUserFromDatabase(user_id)[3].splitlines()
    return files_list

def setUserFilesList(user_id, file_id):
    files_list = getUserFromDatabase(user_id)[3]
    if files_list == '':
        conn, cursor = getCursorBatabase()
        cursor.execute("SELECT * FROM users WHERE user_id=?",(user_id,))
        exists = cursor.fetchall()
        if exists:
            cursor.execute("UPDATE users SET user_files_list=? WHERE user_id=?", (file_id, user_id,))
        conn.commit()
    else:
        files_list = files_list.splitlines()
        new_list = ''
        cicles = 0
        for line in files_list:
            cicles += 1
            if line != '':
                if cicles == 1:
                    new_list = f"{line}"
                else:
                    new_list = f"{new_list}\n{line}"

        new_list = f"{new_list}\n{file_id}"

        conn, cursor = getCursorBatabase()
        cursor.execute("SELECT * FROM users WHERE user_id=?",(user_id,))
        exists = cursor.fetchall()
        if exists:
            cursor.execute("UPDATE users SET user_files_list=? WHERE user_id=?", (new_list, user_id,))
        conn.commit()


def addFileData(file_id, file_full_id, file_name, file_size, file_user_uploader_id):
    file_from_data = getFileData(file_id)
    if file_from_data == False:
        setUserFilesList(file_user_uploader_id, file_id)
        conn, cursor = getCursorBatabase()
        cursor.execute("INSERT INTO files VALUES(?, ?, ?, ?, ?, ?);", (file_id, file_full_id, file_name, file_size, 0, file_user_uploader_id))
        conn.commit()
        return True
    else:
        return file_from_data[0]

def getFileData(file_id):
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM files WHERE file_id=?",(file_id,))
    exists = cursor.fetchall()
    conn.commit()
    if not exists:
        return False
    else:
        return exists[0]

def deleteFileFromDataAndUser(file_id):
    file = getFileData(file_id)
    if file:
        user = getUserFromDatabase(file[5])
        conn, cursor = getCursorBatabase()
        cursor.execute("DELETE from files where file_id=?",(file_id,))
        conn.commit()

        files_list = user[3].splitlines()

        cicles = 0
        new_list = ''
        for line in files_list:
            if line != file_id:
                cicles += 1
                if line != '':
                    if cicles == 1:
                        new_list = f"{line}"
                    else:
                        new_list = f"{new_list}\n{line}"


        user_id = user[0]
        conn, cursor = getCursorBatabase()
        cursor.execute("SELECT * FROM users WHERE user_id=?",(user_id,))
        exists = cursor.fetchall()
        if exists:
            cursor.execute("UPDATE users SET user_files_list=? WHERE user_id=?", (new_list, user_id,))
        conn.commit()


def setFileDownloads(file_id):
    file = getFileData(file_id)
    views = int(file[4]) + 1
    conn, cursor = getCursorBatabase()
    cursor.execute("SELECT * FROM files WHERE file_id=?",(file_id,))
    exists = cursor.fetchall()
    if exists:
        cursor.execute("UPDATE files SET file_downloads=? WHERE file_id=?", (views, file_id,))
    conn.commit()



def getCursorBatabase():
    conn = sqlite3.connect(config.DATABASE)
    cursor = conn.cursor()
    return conn, cursor


def createBataBase():
    conn, cursor = getCursorBatabase()
    cursor.execute("""CREATE TABLE if not exists users(
        user_id INTEGER,
        user_regdata INTEGER,
        user_menu TEXT,
        user_files_list TEXT
        )""")

    cursor.execute("""CREATE TABLE if not exists files(
        file_id TEXT,
        file_full_id TEXT,
        file_name TEXT,
        file_size INTEGER,
        file_downloads INTEGER,
        file_user_uploader_id TEXT
        )""")

    conn.commit()

createBataBase()