from db import init_db, SessionLocal
from models import Transactions, Account
from sqlalchemy.exc import SQLAlchemyError
import csv
import great_expectations as gx
import pandas as pd
import warnings
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, date
from alive_progress import alive_bar
warnings.filterwarnings("ignore", message="`result_format` configured at the Validator-level*")



# def progress_bar(current, percent):
#     temp_count = int(current / percent)
#     bar_percent = int(temp_count / 10)
#     if percent != 0 and current % percent == 0:
#         bar = "█" * bar_percent
#         if temp_count % 10 >= 5:
#             bar = bar + "▌"
#         print(f"{temp_count}% {bar}", )

def main():
    # Load the data
    transaction_df = pd.read_csv("./data/transactions.csv", dtype="string")
    account_df = pd.read_csv("./data/sebank_customers_with_accounts.csv", dtype="string")
    error_columns = list(transaction_df.columns) + ['error notes']
    transaction_error_df = pd.DataFrame(columns=error_columns)

    for column in transaction_df:
        if column == "amount": #removes spacing errors from the amount column
            transaction_df[column] = transaction_df[column].astype(str).str.replace(" ","")
        elif column == "timestamp":
            transaction_df[column] = transaction_df[column].astype(str).str.replace("-","")
            transaction_df[column] = transaction_df[column].astype(str).str.replace(".",":")
        transaction_df[column] = transaction_df[column].astype(str).str.strip()
    #print(transaction_df[column].head())

    #data conversion
    transaction_df["amount"] = pd.to_numeric(transaction_df["amount"], errors="coerce")

    with alive_bar(len(transaction_df)) as bar:
        valid_rows = []
        error_rows = []

        for index, row in transaction_df.iterrows():
            row = row.copy()

            if pd.isna(transaction_df.loc[index, 'notes']):
                row['notes'] = "no notes"

            if row.isna().any(): #checks if any of the columns are empty
                row['error notes'] = "missing data"
                bar()
                continue
            #end of timestamp empty check

            temp_list = str(transaction_df.loc[index,'timestamp']).split()
            temp_date = temp_list[0]
            temp_time = temp_list[1]
            datetime_corrected = False
            if len(temp_list[0]) == 6: #checks if date is written using the full year or 2 digits only
                datetime_corrected = True
                temp_date = "20" + temp_list[0]
                if datetime.strptime(temp_date, "%Y%m%d").date() > date.today():
                    temp_date = "19" + temp_list[0]
                    #end of date length check if-statement

            if len(temp_list[1]) == 5: #checks if timestamp is written using seconds or not
                datetime_corrected = True
                temp_time = temp_list[1] + ":00"
                #end of time length check if-statement

            if datetime_corrected:
                temp_timestamp = temp_date + " " + temp_time
                transaction_df.loc[index,'timestamp'] = temp_timestamp
            try:
                transaction_df.loc[index,'timestamp'] = pd.to_datetime(transaction_df.loc[index,'timestamp'], format="%Y%m%d %H:%M:%S", errors="raise")
            except ValueError as e:
                row['error notes'] = "invalid timestamp"
                error_rows.append(row)
                #progress_bar(index, 1000)
                bar()
                continue
            if row["transaction_type"] == "incoming":
                if account_df["BankAccount"].str.contains(row["receiver_account"]).sum() == 0:
                    row['error notes'] = "receiver account not in database"
                    error_rows.append(row)
                    #progress_bar(index, 1000)
                    bar()
                    continue

            elif row["transaction_type"] == "outgoing":
                if account_df["BankAccount"].str.contains(row["sender_account"]).sum() == 0:
                    row['error notes'] = "sender account not in database"
                    error_rows.append(row)
                    #progress_bar(index, 1000)
                    bar()
                    continue

            else:
                row['error notes'] = "invalid transaction type"
                error_rows.append(row)
                #progress_bar(index, 1000)
                bar()
                continue


            valid_rows.append(row)
            bar()

    transaction_df = pd.DataFrame(valid_rows)
    transaction_error_df = pd.DataFrame(error_rows)
    print(f" successful transaction: {len(transaction_df.index)}")
    print(f" unsuccessful transaction: {len(transaction_error_df.index)}")
    print(f" transaction processed : {len(transaction_df.index) + len(transaction_error_df.index)}")
    print("#==================================================#")
    print(transaction_df.head())
    print(transaction_error_df.head())
    #=== Outputting data ===#
    transaction_error_df.to_csv('data/unsuccessful_transaction.csv',mode='w', header=False, sep=';', decimal =',', index=False)

    # # Create the ephemeral GX context
    # context = gx.get_context()
    #
    # # Add a pandas datasource
    # data_source = context.data_sources.add_pandas(name="pandas")
    #
    # # Add a dataframe asset
    # data_asset = data_source.add_dataframe_asset(name="transactions_data")
    #
    # # Define the batch (entire DataFrame)
    # batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_def")
    # batch = batch_definition.get_batch(batch_parameters={"dataframe": transaction_df})
    #
    # # Create the expectation suite with a name
    # suite = gx.core.expectation_suite.ExpectationSuite(name="transactions_suite")
    #
    # # Get the validator using the suite
    # validator = context.get_validator(batch=batch, expectation_suite=suite)
    #
    # # Add expectations
    # validator.expect_column_values_to_be_between("amount", min_value=0.01, max_value=100000)
    # validator.expect_column_values_to_not_be_null("timestamp")
    #
    # # Validate
    # results = validator.validate()
    #
    # # Print results
    # print(results)

    init_db()
    db = SessionLocal()

    try:
        with db.begin():
            for _, row in account_df.itterows():
                account = Account(
                    customer_name=row['CustomerName'],
                    address=row['Address'],
                    phone_number=row['PhoneNumber'],
                    person_number=row['Personnummer'],
                    account_number=row['BankAccount']
                )
                db.merge(account)

            db.commit()

            for _, row in transaction_df.iterrows():
                transaction = Transactions(
                transaction_id=row['transaction_id'],
                timestamp=row['timestamp'],
                amount=row['amount'],
                currency=row['currency'],
                sender_account=row['sender_account'],
                receiver_account=row['receiver_account'],
                sender_country=row['sender_country'],
                sender_municipality=row['sender_municipality'],
                receiver_country=row['receiver_country'],
                receiver_municipality=row['receiver_municipality'],
                transaction_type=row['transaction_type'],
                notes=row['notes']
            )
                db.add(transaction)

            db.commit()

            print("Data upload successful.")
            print(f"accounts added: {len(account_df)}")
            print(f"transactions added: {len(transaction_df)}")
    except SQLAlchemyError as e:
        db.rollback()
        print("[ERROR] data not uploaded, rolling back. Error Code: ", e)
    finally:
        db.close()

if __name__ == '__main__':
    main()