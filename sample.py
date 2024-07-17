import cx_Oracle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

class Baza:
    def __init__(self):
        try:
            cx_Oracle.init_oracle_client(lib_dir="D:\\Studia\\mag_sem1\\bazy_danych\\laby\\instantclient_21_9")
            dsn_tns = cx_Oracle.makedsn('217.173.198.136', '1521', service_name='whdb')
            self.con = cx_Oracle.connect(user='s98784', password='s98784', dsn=dsn_tns)
            print(f"Connection provided. Connection version: {self.con.version}\n")

        except cx_Oracle.DatabaseError as e:
            print("There is a problem with Oracle: ", e)
            if self.con:
                self.con.close()


    def show(self):
        try:
            cursor = self.con.cursor()
            cursor.execute(f"select * from Chirurgia")

            lekarz = pd.read_sql("select * from Lekarz", con=self.con)
            urlop = pd.read_sql("select * from Urlop", con=self.con)
            wizyta = pd.read_sql("select * from Wizyta", con=self.con)
            pacjent = pd.read_sql("select * from Pacjent", con=self.con)

            colors = ['c', 'orange', 'red', 'black']
            print("\n")

            plt.figure(figsize=(8, 7))
            for i in range(15):
                if i % 2 == 0:
                    plt.bar(lekarz['NAZWISKO'][i], lekarz['WYPLATA'][i], color=colors[0])
                else:
                    plt.bar(lekarz['NAZWISKO'][i], lekarz['WYPLATA'][i], color=colors[1])
            plt.xlabel('Imię pracownika')
            plt.ylabel('Wielkość wypłaty')
            plt.title('Wypłaty lekarzy')
            plt.xticks(rotation=60)
            plt.legend(title="Wypłaty lekarzy w zł", loc=0)
            plt.show()

            all = lekarz.merge(wizyta, left_on='ID', right_on='LEKARZ_ID')
            # print(all)
            plt.figure(figsize=(8, 7))
            for i in range(15):
                if i % 2 == 0:
                    plt.scatter(all['NAZWISKO'][i], all['DATA'][i], color=colors[2])
                else:
                    plt.scatter(all['NAZWISKO'][i], all['DATA'][i], color=colors[3])
            plt.xlabel('Nazwisko')
            plt.ylabel('Data')
            plt.title('Wizyty lekarzy')
            plt.xticks(rotation=60)
            plt.legend(title="Data wizyt lekarzy", loc=0)
            plt.show()

            urlopy = lekarz.merge(urlop, left_on='ID', right_on='LEKARZ_ID')
            print(urlopy)

            fig, ax1 = plt.subplots(figsize=(8, 7))
            ax2 = ax1.twinx()
            ax1.scatter(urlopy['NAZWISKO'], urlopy['DATA_OD'], color='b', label='Data rozpoczęcia urlopu')
            ax2.scatter(urlopy['NAZWISKO'], urlopy['DATA_DO'], color='r', label='Data zakończenia urlopu')
            ax1.set_ylabel('Data rozpoczęcia urlopu')
            ax2.set_ylabel('Data zakończenia urlopu')
            plt.title('Urlopy lekarzy')
            plt.xlabel('Nazwisko lekarza')
            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines + lines2, labels + labels2, loc=0)
            plt.xticks(rotation=60)
            plt.xticks(rotation=60)
            plt.show()

            pacjent = pacjent.merge(wizyta, left_on='ID', right_on='PACJENT_ID')
            pacjenci = pacjent.iloc[:15]

            print(pacjenci)

            plt.figure(figsize=(8, 7))
            sns.barplot(pacjenci, x='NAZWISKO', y='CENA', color=colors[0])
            plt.gca().set_ylim([0, pacjenci['CENA'].max()])
            plt.title('Ceny za wizytę pacjentów')
            plt.xlabel('Nazwisko pacjenta')
            plt.ylabel('Cena')
            plt.xticks(rotation=60)
            plt.legend(title="Ceny wizyt pacjentów", loc=0)
            plt.show()

            fig = px.scatter(pacjenci, x='NAZWISKO', y='DATA', title='Data wizyt pacjentów', labels={'NAZWISKO': 'Nazwisko pacjenta', 'DATA': 'Data wizyty'})
            fig.show()

            group = all.groupby('LEKARZ_ID').count()['ID_x'].reset_index()
            group.columns = ['LEKARZ_ID', 'LICZBA WIZYT']
            print(group)

        except cx_Oracle.Error as e:
            ("Error occured: ", e)

        finally:
            if cursor:
                cursor.close()


    def close_conn(self):
        if self.con:
            self.con.close()
            print("\nConnection closed.\n")

if __name__ == '__main__':
    baza = Baza()
    baza.show()
    baza.close_conn()
