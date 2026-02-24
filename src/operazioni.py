from pyspark.sql.functions import col, when, length


class Operazioni:
    '''Static method = Significa che puoi chiamare il metodo senza creare un oggetto della classe!'''

    @staticmethod
    def is_number(df, col_name):
        return df.withColumn(
            f"valid_{col_name}_number",
            when(col(col_name).rlike("^[0-9]+(\\.[0-9]+)?$"), True).otherwise(False)
        )

    @staticmethod
    def positive_number(df, col_name):
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).rlike("^[0-9]+(\\.[0-9]+)?$") & (col(col_name).cast("double") >= 0), True).otherwise(
                False)
        )

    @staticmethod
    def string_no_numbers(df, col_name):
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).isNotNull() & (~col(col_name).rlike("[0-9]")), True).otherwise(False)
        )

    @staticmethod
    def date_format_regex(df, col_name):
        # Estrazione anno, mese, giorno come interi
        anno = col(col_name).substr(1, 4).cast("int")
        mese = col(col_name).substr(6, 2).cast("int")
        giorno = col(col_name).substr(9, 2).cast("int")

        # Funzione di validazione mese/giorno
        valid_day = (
                ((mese.isin([1, 3, 5, 7, 8, 10, 12])) & (giorno.between(1, 31))) |  # mesi con 31 giorni
                ((mese.isin([4, 6, 9, 11])) & (giorno.between(1, 30))) |  # mesi con 30 giorni
                ((mese == 2) & (giorno.between(1, 28))) |  # febbraio normale
                ((mese == 2) & (giorno == 29) & ((anno % 4 == 0) & ((anno % 100 != 0) | (anno % 400 == 0))))
        # bisestile
        )

        return df.withColumn(
            f"valid_{col_name}",
            when(
                (col(col_name).rlike(r'^\d{4}-\d{2}-\d{2}$')) & valid_day,
                True
            ).otherwise(False)
        )

    @staticmethod
    def email(df, col_name):
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).rlike(pattern), True).otherwise(False)
        )

    @staticmethod
    def string_length(df, col_name, length_val):
        """
        Controlla che la colonna col_name abbia esattamente length_val caratteri.
        """
        return df.withColumn(
            f"valid_{col_name}",
            when(
                col(col_name).isNotNull() & (length(col(col_name)) == length_val),
                True
            ).otherwise(False)
        )
    
    @staticmethod
    def foreign_key(df, col_name, table_name):
        """
        Controlla che la colonna col_name contenga solo valori contenuti nella colonna
        con lo stesso nome della tabella table_name
        """
        # leggere colonna da tabella 

        # ritornare df con colonna valid se il valore e presente - per il controllo
        # fare una left join con la tabella usando quella colonna, se il valore della 
        # colonna dell'altra tavola e nullo, valid_ sara False, altrimenti true

    @staticmethod
    def primary_key(df, col_name):
        """
        Controlla che la colonna col_name possa essere una PK, ossia che non contenga
        valori doppi o nulli
        """
        # check if there are null or multiple values, and raise error if so

    @staticmethod
    def equivalence(df, new_col, old_col):
        """
        Controlla che il vecchio valore di una colonna sia uguale al nuovo
        """
        return df.withColumn(
            f"change_{new_col}",
            when(
               col(new_col) != col(old_col),
               True
            ).otherwise(False)
        )
    
    @staticmethod
    def greater_than(df, col1, col2):
        """
        Controlla che il valore in col1 sia maggiore del valore in col2
        """
        return df.withColumn(
            f"greater_{col1}",
            when(
                col(col1) > col(col2),
                True
            ).otherwise(False)
        )
        