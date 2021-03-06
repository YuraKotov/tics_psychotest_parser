import requests
import csv
import pymysql

# Получение результатов из БД
def from_database():
    connection = pymysql.connect(

        host = "",
        user = "",
        password = "",
        db = "",
        charset = ""
    )

    cur = connection.cursor()
    cur.execute("SELECT * FROM schwartztest") 
    schw_rows = cur.fetchall()

    cur.execute("SELECT * FROM kettelltest") 
    ket_rows = cur.fetchall()

    cur.execute("SELECT * FROM defensetest") 
    def_rows = cur.fetchall()

    connection.close()


    # Пожинаем Шварца
    data_schw = []
    for row in schw_rows:
        is_exist = False
        for data in data_schw:
            if row[20] == data[0]:
                is_exist = True
        if is_exist == False:
            data_schw.append([row[20], [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], \
                                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19]]])

    # Пожинаем Кеттелла
    data_ket = []
    for row in ket_rows:
        is_exist = False
        for data in data_ket:
            if row[17] == data[0]:
                is_exist = True
        if is_exist == False:
            data_ket.append([row[17], [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], \
                                        row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16]]])

    # Пожинаем псих.защиту
    data_def = []
    for row in def_rows:
        is_exist = False
        for data in data_def:
            if row[10] == data[0]:
                is_exist = True
        if is_exist == False:
            data_def.append([row[10], [row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]]])
    

    # Добавление результатов Кеттелла
    db_data = data_def.copy()
    for ket in data_ket:
        is_exist = False
        for db in db_data:
            if db[0] == ket[0]:
                db.append(ket[1])
                is_exist = True
        if is_exist == False:
            db_data.append([ket[0], ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], ket[1]])
    
    for data in db_data:
        if len(data) < 3:
            data.append(['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'])

    # Добавление результатов Шварца
    for schw in data_schw:
        is_exist = False
        for db in db_data:
            if db[0] == schw[0]:
                db.append(schw[1])
                is_exist = True
        if is_exist == False:
            db_data.append([schw[0], ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], ['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'], schw[1]])
    
    for data in db_data:
        if len(data) < 4:
            data.append(['un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un', 'un'])

    
    # Тестовый вывод
    for data in db_data:
        print(data[0], data[1], data[2], data[3])
        print()

    return db_data

# Получение данных из профилей
def take_data(db_data):

    token = ''
    user_token = ''
    version = 5.89
    all_data = []

    list_id = []
    for item in db_data:
        list_id.append(item[0])

    for id in list_id:
        response = requests.get('https://api.vk.com/method/users.get',
                                params={
                                    'access_token': token,
                                    'v': version,
                                    'user_id': id,
                                    'fields': 'sex,bdate,career,city,educations,occupation,personal,relation,schools,universities'
                                    }
                                )

        data = response.json()['response'][0]

        response = requests.get('https://api.vk.com/method/users.get', 
                                params={
                                    'access_token': user_token,
                                    'v': version,
                                    'user_id': id,
                                    'fields': 'counters'
                                    }
                                )
        counters = response.json()['response'][0]['counters']

        all_data.append({ 'vk': data, 'counters': counters })

    return all_data

# Запись данных в CSV-файл
def file_writer(db_data, data):
    i = 0
    with open('psycho_datasets.csv', 'w', encoding='utf-8') as file:
        a_pen = csv.writer(file)
        a_pen.writerow(('id', 'first_name', 'last_name', 'is_closed', 'sex', 'bdate', 'friends', 'followers', 'photos', 'audios', 'pages', 'occupation', 'def1', 'def2', 'def3', 'def4', 'def5', 'def6', 'def7', 'def8', 'def9', \
                        'ket1', 'ket2', 'ket3', 'ket4', 'ket5', 'ket6', 'ket7', 'ket8', 'ket9', 'ket10', 'ket11', 'ket12', 'ket13', 'ket14', 'ket15', 'ket16', \
                        'rang1-1', 'rang1-2', 'rang1-3', 'rang1-4', 'rang1-5', 'rang1-6', 'rang1-7', 'rang1-8', 'rang1-9', 'rang1-10', 'rang2-1', 'rang2-2', 'rang2-3', 'rang2-4', 'rang2-5', 'rang2-6', 'rang2-7', 'rang2-8', 'rang2-9', 'rang2-10'))
        for user in data:
            a_pen.writerow((user['vk']['id'], user['vk']['first_name'], user['vk']['last_name'], user['vk']['is_closed'], user['vk']['sex'], user['vk']['bdate'] if 'bdate' in user['vk'] else "undefined", \
                            user['counters']['friends'], user['counters']['followers'] if 'followers' in user['counters'] else 'undefined', user['counters']['photos'] if 'photos' in user['counters'] else 'undefined', \
                            user['counters']['audios'] if 'audios' in user['counters'] else 'undefined', user['counters']['pages'] if 'pages' in user['counters'] else 'undefined', user['vk']['occupation']['name'] if 'occupation' in user['vk'] else 'undefined', \
                            db_data[i][1][0], db_data[i][1][1], db_data[i][1][2], db_data[i][1][3], db_data[i][1][4], db_data[i][1][5], db_data[i][1][6], db_data[i][1][7], db_data[i][1][8], \
                            db_data[i][2][0], db_data[i][2][1], db_data[i][2][2], db_data[i][2][3], db_data[i][2][4], db_data[i][2][5], db_data[i][2][6], db_data[i][2][7], db_data[i][2][8], db_data[i][2][9], db_data[i][2][10], db_data[i][2][11], db_data[i][2][12], db_data[i][2][13], db_data[i][2][14], db_data[i][2][15], \
                            db_data[i][3][0], db_data[i][3][1], db_data[i][3][2], db_data[i][3][3], db_data[i][3][4], db_data[i][3][5], db_data[i][3][6], db_data[i][3][7], db_data[i][3][8], db_data[i][3][9], db_data[i][3][10], db_data[i][3][11], db_data[i][3][12], db_data[i][3][13], db_data[i][3][14], db_data[i][3][15], db_data[i][3][16], db_data[i][3][17], db_data[i][3][18], db_data[i][3][19]))
            i += 1
            

db_data = from_database()
all_data = take_data(db_data)
file_writer(db_data, all_data)