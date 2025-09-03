#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd





# In[2]:


enrollment = pd.read_csv("clean_data.csv")

# Title I allocations (converted from PDF)
title1 = pd.read_csv("newjerseypdf-40553.csv")

# NCES crosswalk - contains mapping for LEAID to ID 
crosswalk = pd.read_csv("ccd_lea_029_2324_w_1a_073124.csv", low_memory=False)


title1["Title1 Allocations"] = (
    title1["Title1 Allocations"]
    .replace({",": ""}, regex=True)
    .astype(int)
)

# Keep only funded districts (allocations > 0)
title1_filtered = title1[title1["Title1 Allocations"] > 0].copy()



# In[3]:


crosswalk_nj = crosswalk[crosswalk["ST"] == "NJ"].copy()

# Extract the last 4 digits of ST_LEAID (to match enrollment "District Code")
crosswalk_nj["District Code"] = crosswalk_nj["ST_LEAID"].str[-4:].astype(str)

# Ensure both codes are strings
enrollment["District Code"] = enrollment["District Code"].astype(str)

#joining enrollment data with the crosswalk data to get LEAID
enroll_xwalk = enrollment.merge(
    crosswalk_nj[["LEAID", "District Code", "LEA_NAME"]],
    on="District Code",
    how="left"
)

enroll_xwalk = enroll_xwalk.dropna(subset=["LEAID"]).copy()



#joining the previous data with the title allocations  data to get districts with title1 allocations
final_merged = enroll_xwalk.merge(
    title1_filtered,
    left_on="LEAID",
    right_on="LEA ID",
    how="inner"
)


final_merged["Grades_5_8"] = (
    final_merged["Fifth Grade"] +
    final_merged["Sixth Grade"] +
    final_merged["Seventh Grade"] +
    final_merged["Eighth Grade"]
)

# county level aggregation - getting the county level data for grades 5 to 8
county_summary = (
    final_merged.groupby("County Name")["Grades_5_8"]
    .sum()
    .reset_index()
    .sort_values("Grades_5_8", ascending=False)
)

# district and school level aggregation - getting the  data for grades 5 to 8
school_summary = (
    final_merged.groupby(["District Name", "School Name"])["Grades_5_8"]
    .sum()
    .reset_index()
    .sort_values(["District Name", "Grades_5_8"], ascending=[True, False])
)



# In[4]:


# Converting the dataframes to csv files
county_summary.to_csv("county_summary.csv", index=False)
school_summary.to_csv("school_summary.csv", index=False)

# Getting the number of rows of each dataframes
print("Enrollment rows:", enrollment.shape[0])
print("Crosswalk (NJ) rows:", crosswalk_nj.shape[0])
print("Title I rows:", title1_filtered.shape[0])
print("Final merged rows:", final_merged.shape[0])

print("\nTop 5 Counties by Grades 5–8 :")
print(county_summary.head())

print("\nTop 5 Schools for each district:")
print(school_summary.head())


# In[7]:


county_summary


# In[6]:


school_summary


# In[ ]:




