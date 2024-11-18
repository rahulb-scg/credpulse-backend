import numpy as np
import pandas as pd
import os

# Take a specific dataset from the data as required (ToDo: Add to Utils)

def data_sampler(df):
    print("Current shape of data: ", df.shape)
    
    # To remove a group with certain values

    # Filtering to keep data of only 1 loan term
    print("Unique Loan Terms: ", df['ORIG_TERM'].unique())
    values_to_remove= [180, 240, 120, 300, 324, 168, 288, 312, 144, 336, 179, 216, 276, 156, 228, 236, 204, 306, 
                       264, 348, 198, 282, 311, 270, 252, 181, 150, 328, 155, 290,345,237,339,347,342,313,239]
    
    df = df.groupby('ORIG_TERM').filter(lambda x: not any(x['ORIG_TERM'].isin(values_to_remove)))
    print("Loan Terms considered: ", df['ORIG_TERM'].unique())
    
    print("Current shape of data: ", df.shape)

    # Creating a sample of 50% of this data
    
    print("Sampling data: ", df.shape, end='\n\n')
    
    # Step 1: Get unique Loan IDs and sample the specified fraction of them
    loan_ids = df['LOAN_ID'].unique()
    sampled_loan_ids = pd.Series(loan_ids).sample(frac=0.5, random_state=42) # @Todo Make Dynamic

    # Step 2: Filter the original dataframe to include only the rows with the sampled Loan IDs
    sampled_df = df[df['LOAN_ID'].isin(sampled_loan_ids)]
    print("Sampled data's shape: ", sampled_df.shape)

    return sampled_df


def filter_df(loan_id):
    loan_id.reset_index(drop=True, inplace=True)
    start_idx = loan_id.loc[loan_id['DLQ_STATUS'] == 4].index
    
    if not start_idx.empty:
        start_idx = start_idx[0] + 1  # Get the first index where DLQ_STATUS is 4
        end_idx = loan_id.index[-1]
        loan_id.drop(index=range(start_idx, end_idx + 1), inplace=True)
    return loan_id


#   Feature Engneering
#   The function gets the loan data it takes down the required columns ans then we return the Feature Engineered loan_data
def feature_engg(df):
    df_feature = df.copy()

    required_cols = ['LOAN_ID' , 'ACT_PERIOD' , 'ORIG_UPB' , 'CURRENT_UPB' , 'DLQ_STATUS']
    print("Filtering columns as required by the model...", "\n", required_cols)
    # Taking only relevant columns
    df_feature = df_feature[required_cols]

    # Remove rows after DLQ Status is 4 marking as charge-off
    print("Unique DLQ Status in the dataset: ", df_feature['DLQ_STATUS'].unique())
    df_feature = df_feature.groupby('LOAN_ID').apply(filter_df).reset_index(drop=True)
    values_to_remove = [99,12,7]
    df_feature = df_feature.groupby('DLQ_STATUS').filter(lambda x: not any(x['DLQ_STATUS'].isin(values_to_remove)))

    print("Unique DLQ Status in the dataset after filtering", df_feature['DLQ_STATUS'].unique())
    print("Current shape of data: ", df_feature.shape)

    # Mapping the Delq_Bucket
    dqm_mapping = {0:'Current' , 1:'30 DPD' , 2:'60 DPD' , 3:'90 DPD' , 4:'Charged Off'}
    print("Mapping Delq Buckets...")

    # Apply the mapping function to create a new column 'days_past_due'
    df_feature['DAYS_PAST_DUE'] = df_feature['DLQ_STATUS'].map(dqm_mapping)
    print("Creating 'Days Past Due' Column...")

    # Mapping to Loan_status
    loan_status = {0 : 'Current', 1:'30 DPD', 2:'60 DPD', 3:'90 DPD', 4: 'Charged Off'}
    print("Creating 'Derived Loan Status' Column...")
    df_feature['DERIVED_LOAN_STATUS'] = df_feature['DLQ_STATUS'].map(loan_status)
    print(df_feature['DERIVED_LOAN_STATUS'].unique())

    # Creating Next_Loan_Drived_Status
    print("Creating 'Next Derived Loan Status' Column...")
    df_feature['NEXT_DERIVED_LOAN_STATUS'] = df_feature.groupby('LOAN_ID')['DERIVED_LOAN_STATUS'].shift(-1).ffill()

    # Ensuring clean data
    print("Further Cleaning...")
    df_feature.loc[df_feature['DERIVED_LOAN_STATUS'] == 'Charged Off', 'NEXT_DERIVED_LOAN_STATUS'] = 'Charged Off'
    
    # Creating Next DPD Status
    df_feature['NEXT_DAYS_PAST_DUE'] = df_feature.groupby('LOAN_ID')['DAYS_PAST_DUE'].shift(-1).ffill()

    # Creating a new column with charged-off amount
    print("Creating 'Charge off Amount' Column...")
    df_feature['CHARGE_OFF_AMT'] = df_feature.apply(lambda x: x['CURRENT_UPB'] if x['DERIVED_LOAN_STATUS'] == 'Charged Off' else 0, axis=1)
    
    print("Changin value for unpaid balance where charge_off is applicable...")
    df_feature.loc[df_feature['DERIVED_LOAN_STATUS'] == 'Charge Off', 'CURRENT_UPB'] = 0

    return df_feature


def Cgl_Curve(distribution, transition_matrix):
    Cgl_Curve = []
    for i in range(13):
        state_probability = np.dot(distribution, np.linalg.matrix_power(transition_matrix, i))
        Cgl_Curve.append([f"Period_{i}"] + state_probability.tolist())

    # Create DataFrame with the first column as "Period" and the rest based on the distribution index
    df1 = pd.DataFrame(Cgl_Curve, columns=["Period"] + distribution.index.tolist())

    # Set "Period" as the index of the DataFrame
    df1.set_index("Period", inplace=True)

    # To calculate the Monthly Default Rate
    df1['MONTHLY_DEFAULT_RATE'] = df1['Charged Off'].diff()

    return df1


def calculator(df):
  
    transition_matrix = pd.pivot_table(df, values='CURRENT_UPB', index='DERIVED_LOAN_STATUS', columns='NEXT_DERIVED_LOAN_STATUS', aggfunc='count', sort=False , fill_value = 0).pipe(lambda x: x.div(x.sum(axis = 1),axis = 0))
    print("Created transition matrix..")

    # Current Distribution
    distribution = df.groupby(['LOAN_ID']).apply(lambda x:x.iloc[-1]).groupby(['DERIVED_LOAN_STATUS'])['CURRENT_UPB'].sum().pipe(lambda x:x/x.sum()).loc[['Current', '30 DPD', '60 DPD', '90 DPD' , 'Charged Off']]
    print("Created Distribution..")
    
    CglCurve = Cgl_Curve(distribution , transition_matrix)
    print("Created CGL Curve..")
    
    ALLL = CglCurve['Charged Off'][12] - CglCurve['Charged Off'][0]
    
    CECL= ALLL*1.5
    
    return {'Transition_Matrix':transition_matrix , 'Distribution':distribution , 'CGL_Curve' : CglCurve ,'ALLL':ALLL ,'CECL' : CECL}


def run_model(df):
   
    print("Preparing data for model...")

    loan_data = data_sampler(df)
    loan_data = feature_engg(df)

    print("Exporting Model-Ready Data to CSV..")

    # Attempt to write the DataFrame to a CSV file
    try:
        loan_data.to_csv('test/test_data/Model Ready/TMM1_360_bucket.csv')
        print("File saved successfully.")
    except FileNotFoundError as fnf_error:
        print(f"FileNotFoundError: {fnf_error}")
    except PermissionError as p_error:
        print(f"PermissionError: {p_error}")
    except IsADirectoryError as dir_error:
        print(f"IsADirectoryError: {dir_error}")
    except OSError as os_error:
        print(f"OSError: {os_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


    output = calculator(loan_data)

    return output

if __name__ == '__main__':
    run_model()