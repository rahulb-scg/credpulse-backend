# Standard library imports
import os

# Third-party imports
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Local imports
from backend.models import tmm1_data

# Load environment variables
load_dotenv()

# Get path from environment variables with fallback defaults
OUTPUT_DIR = os.getenv('TMM1_OUTPUT_DIR', 'backend/test/test_data/Model Ready')
OUTPUT_FILE = os.getenv('TMM1_OUTPUT_FILE', 'TMM1_360_bucket.csv')

# Take a specific dataset from the data as required (ToDo: Add to Utils)

def data_sampler(df):
    print("Current shape of data: ", df.shape)
    
    # STEP 1: Filter by loan term - keep all records for loans with desired terms
    print("Unique Loan Terms: ", df['ORIG_TERM'].unique())
    values_to_keep = [360]  # specify the values you want to keep
    
    # Get loan IDs where the loan term matches our criteria
    valid_loan_ids = df[df['ORIG_TERM'].isin(values_to_keep)]['LOAN_ID'].unique()
    # Filter to keep all records for these loan IDs
    df = df[df['LOAN_ID'].isin(valid_loan_ids)]
    
    print("Loan Terms considered: ", df['ORIG_TERM'].unique())
    print("Current shape of data: ", df.shape)

    # STEP 2: Sample the filtered data
    print("Sampling data: ", df.shape, end='\n\n')
    
    # Get unique Loan IDs and sample 50% of them
    loan_ids = df['LOAN_ID'].unique()
    sampled_loan_ids = pd.Series(loan_ids).sample(frac=0.5, random_state=42)

    # Filter dataframe to only include sampled Loan IDs
    sampled_df = df[df['LOAN_ID'].isin(sampled_loan_ids)]
    print("Sampled data's shape: ", sampled_df.shape)

    return sampled_df

#   Feature Engneering
#   The function gets the loan data it takes down the required columns ans then we return the Feature Engineered loan_data
def feature_engg(df, data_config):
    df_feature = df.copy()

    print(df_feature['DLQ_STATUS'].unique())
    df_feature = tmm1_data.prepare(df_feature, data_config)
    print(df_feature.shape, "\n")

    # Get bucket configuration
    bucket_config = data_config['configuration']['loan_buckets']
    bucket_map = bucket_config['bucket_map']

    # Remove rows after DLQ Status is x marking as charged-off
    print("Unique DLQ Status in the dataset: ", df_feature['DLQ_STATUS'].unique())
    print("Current shape of data: ", df_feature.shape)

    # Create dynamic mappings from config
    print("Creating dynamic mappings from configuration...")
    days_past_due_mapping = {}
    loan_status_mapping = {}
    
    for status_code, status_name in bucket_map.items():
        days_past_due_mapping[status_code] = status_name
        loan_status_mapping[status_code] = status_name

    print("Mapping Delq Buckets...")
    # Apply the mapping function to create a new column 'days_past_due'
    df_feature['DAYS_PAST_DUE'] = df_feature['DLQ_STATUS'].astype(str).map(days_past_due_mapping)
    print("Creating 'Days Past Due' Column...")

    print("Creating 'Derived Loan Status' Column...")
    df_feature['DERIVED_LOAN_STATUS'] = df_feature['DLQ_STATUS'].astype(str).map(loan_status_mapping)
    print(df_feature['DERIVED_LOAN_STATUS'].unique())

    # Creating Next_Loan_Derived_Status
    print("Creating 'Next Derived Loan Status' Column...")
    df_feature['NEXT_DERIVED_LOAN_STATUS'] = df_feature.groupby('LOAN_ID')['DERIVED_LOAN_STATUS'].shift(-1).ffill()

    # Get the charged off status name from config
    charged_off_status = next((v for k, v in bucket_map.items() if 'charge' in v.lower() or 'default' in v.lower()), 
                            list(bucket_map.values())[-1])  # fallback to last status if no charged off found

    # Ensuring clean data
    print("Further Cleaning...")
    df_feature.loc[df_feature['DERIVED_LOAN_STATUS'] == charged_off_status, 'NEXT_DERIVED_LOAN_STATUS'] = charged_off_status
    
    # Creating Next DPD Status
    df_feature['NEXT_DAYS_PAST_DUE'] = df_feature.groupby('LOAN_ID')['DAYS_PAST_DUE'].shift(-1).ffill()

    # Creating a new column with charged-off amount
    print("Creating 'Charged off Amount' Column...")
    df_feature['CHARGE_OFF_AMT'] = df_feature.apply(
        lambda x: x['CURRENT_UPB'] if x['DERIVED_LOAN_STATUS'] == charged_off_status else 0, 
        axis=1
    )
    
    print("Changing value for unpaid balance where charge_off is applicable...")
    df_feature.loc[df_feature['DERIVED_LOAN_STATUS'] == charged_off_status, 'CURRENT_UPB'] = 0

    return df_feature


def Cgl_Curve(distribution, transition_matrix, prediction_months):
    Cgl_Curve = []
    for i in range(prediction_months):
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

def calculator(df, data_config):
    # Create transition matrix
    transition_matrix = pd.pivot_table(df, 
                                     values='CURRENT_UPB', 
                                     index='DERIVED_LOAN_STATUS', 
                                     columns='NEXT_DERIVED_LOAN_STATUS', 
                                     aggfunc='count', 
                                     sort=False, 
                                     fill_value=0).pipe(lambda x: x.div(x.sum(axis=1), axis=0))
    print("Created transition matrix..")

    # Get bucket values from config
    bucket_values = list(data_config['configuration']['loan_buckets']['bucket_map'].values())
    prediction_months = data_config['configuration']['prediction_months'] + 1
    weighted_average_remaining_life = data_config['configuration']['WARL']
    # Current Distribution using dynamic bucket values
    distribution = (df.groupby(['LOAN_ID'])
                     .apply(lambda x: x.iloc[-1])
                     .groupby(['DERIVED_LOAN_STATUS'])['CURRENT_UPB']
                     .sum()
                     .pipe(lambda x: x/x.sum())
                     .loc[bucket_values])
    print("Created Distribution..")
    
    CglCurve = Cgl_Curve(distribution, transition_matrix, prediction_months)
    print("Created CGL Curve..")
    
    # Allowance for Loans and Lease Losses
    ALLL = CglCurve['Charged Off'][prediction_months-1] - CglCurve['Charged Off'][0]
    
    # Calculate CECL Factor
    CECL = ALLL * weighted_average_remaining_life
    
    # Calculate Ending Balance of snapshot
    # Get last row of each loan group
    last_upb = df.groupby('LOAN_ID').last()['CURRENT_UPB'].sum()
    # Get charged off amount for each loan group
    charged_off = df.groupby('LOAN_ID').last()['CHARGE_OFF_AMT'].sum()
    # Calculate ending balance as sum of last UPB and charged off amounts
    ending_balance = last_upb + charged_off

    # Calculate CECL Amount
    CECL_Amount = CECL * ending_balance

    return {
        'Transition_Matrix': transition_matrix,
        'Distribution': distribution,
        'CGL_Curve': CglCurve,
        'ALLL': ALLL,
        'CECL_Factor': CECL,
        "WARL": weighted_average_remaining_life,
        "CECL_Amount": CECL_Amount,
        "Opening_Balance": df['CURRENT_UPB'].sum(),
        "Ending_Balance": ending_balance
    }


def run_model(df, data_config):
    print("Preparing data for model...")
    
    filtered_loan_data = data_sampler(df)
    feature_engineered_loan_data = feature_engg(filtered_loan_data, data_config)

    # print("Exporting Model-Ready Data to CSV..")
    
    # output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    # try:
    #     loan_data.to_csv(output_path)
    #     print(f"File saved successfully to {output_path}")
    # except Exception as e:
    #     print(f"An error occurred while saving the file: {e}")

    calculator_output = calculator(feature_engineered_loan_data, data_config)
    # output_with_visuals = visualiser(calculator_output)

    return calculator_output

if __name__ == '__main__':
    run_model()