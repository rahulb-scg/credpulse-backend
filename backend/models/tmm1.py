import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from backend.models import tmm1_data
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get path from environment variables with fallback defaults
OUTPUT_DIR = os.getenv('TMM1_OUTPUT_DIR', 'backend/test/test_data/Model Ready')
OUTPUT_FILE = os.getenv('TMM1_OUTPUT_FILE', 'TMM1_360_bucket.csv')

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

#   Feature Engneering
#   The function gets the loan data it takes down the required columns ans then we return the Feature Engineered loan_data
def feature_engg(df, data_config):
    df_feature = df.copy()

    print(df_feature.shape, "\n")
    df_feature = tmm1_data.prepare(df_feature, data_config)
    print(df_feature.shape, "\n")

    # Remove rows after DLQ Status is x marking as charged-off
    print("Unique DLQ Status in the dataset: ", df_feature['DLQ_STATUS'].unique())
    # df_feature = df_feature.groupby('LOAN_ID').apply(filter_df).reset_index(drop=True)

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
    print("Creating 'Charged off Amount' Column...")
    df_feature['CHARGE_OFF_AMT'] = df_feature.apply(lambda x: x['CURRENT_UPB'] if x['DERIVED_LOAN_STATUS'] == 'Charged Off' else 0, axis=1)
    
    print("Changin value for unpaid balance where charge_off is applicable...")
    df_feature.loc[df_feature['DERIVED_LOAN_STATUS'] == 'Charged Off', 'CURRENT_UPB'] = 0

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


def visualiser(output_before_visuals):
    output_after_visuals = output_before_visuals

    def visual1(output_after_visuals = output_before_visuals):

        # Create Plot 
        plt.figure(figsize=(12,6)) 
        plt.plot(output_after_visuals['CGL_Curve']['Charged Off'], marker='o') 
        plt.xlabel('Time Periods') 
        plt.ylabel('Cumulative Gross Loss (CGL)') 
        plt.title('Cumulative Gross Loss (CGL)') 
        plt.grid(True) 
        
        visual = plt.gcf()
        output_after_visuals['CGL'] = visual
        plt.close()

    def visual2(output_after_visuals = output_before_visuals):

        # Plot the Cumulative Charged Off curve with Period on the x-axis
        plt.figure(figsize=(20, 6))
        plt.plot(output_after_visuals['CGL_Curve'].index, output_after_visuals['CGL_Curve']['MONTHLY_DEFAULT_RATE'], marker='o')
        plt.xlabel('Periods')
        plt.ylabel('Monthly Default Rate')
        plt.title('Monthly Default Rate')
        plt.grid(True)
        
        visual = plt.gcf()
        output_after_visuals['Monthly Default Rate'] = visual
        plt.close()

    visual1()
    visual2()
    return output_after_visuals

def calculator(df):
  
    transition_matrix = pd.pivot_table(df, values='CURRENT_UPB', index='DERIVED_LOAN_STATUS', columns='NEXT_DERIVED_LOAN_STATUS', aggfunc='count', sort=False , fill_value = 0).pipe(lambda x: x.div(x.sum(axis = 1),axis = 0))
    print("Created transition matrix..")

    # Current Distribution
    distribution = df.groupby(['LOAN_ID']).apply(lambda x:x.iloc[-1]).groupby(['DERIVED_LOAN_STATUS'])['CURRENT_UPB'].sum().pipe(lambda x:x/x.sum()).loc[['Current', '30 DPD', '60 DPD', '90 DPD' , 'Charged Off']]
    print("Created Distribution..")
    
    CglCurve = Cgl_Curve(distribution , transition_matrix)
    print("Created CGL Curve..")
    
    ALLL = CglCurve['Charged Off'][12] - CglCurve['Charged Off'][0]
    
    CECL = ALLL*1.5
    
    return {'Transition_Matrix':transition_matrix , 'Distribution':distribution , 'CGL_Curve' : CglCurve ,'ALLL':ALLL ,'CECL' : CECL}


def run_model(df, data_config):
    print("Preparing data for model...")
    
    loan_data = data_sampler(df)
    # print("Exporting Model-Ready Data to CSV..")
    # loan_data.to_csv('test/test_data/Model Ready/TMM1_360_bucket_3.csv')
    loan_data = feature_engg(loan_data, data_config)

    # print("Exporting Model-Ready Data to CSV..")
    
    # output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    # try:
    #     loan_data.to_csv(output_path)
    #     print(f"File saved successfully to {output_path}")
    # except Exception as e:
    #     print(f"An error occurred while saving the file: {e}")

    calculator_output = calculator(loan_data)
    # output_with_visuals = visualiser(calculator_output)

    return calculator_output

if __name__ == '__main__':
    run_model()