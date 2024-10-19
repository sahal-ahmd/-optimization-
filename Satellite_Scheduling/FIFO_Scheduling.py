"""
FIFO Scheduling Algorithm from Publication - DOI: 10.2514/1.I010620

Refer the README-FIFO_Scheduling document for more info on the script.

DataFrame is used as the preffered data structure because of the ease of working and ability to
handle and process huge amounts of data

Inputs expected & Data types:
1. Download Candidate Intervals (DwInt) - Dictionary:
Data format is a dictionary of Pandas DataFrame for every satellite given by the structure
-index- | -Dwd_Task_ID- | -Ground_Station_ID- | -Download_SetUp_Time- | -Download_Start_Time- | -Download_Processing_Time- 
the identifier (key) for the dataframes in the dictionary (DwInt) is the Satellite ID

2. Satellite Informations (SatInfo):
Data format is assumed to be a Pandas DataFrame of the structure
-index- | -Satellite_ID- | -c1- | -c2- | -SatLocation_Long- | -SatLocation_lat- | -Sat_Altitude- | -d_min- | -d_max- | -d_status- | -e_min- | -e_max- | -e_status- 
The information on c1, c2, d_x's and e_x's can be found in the same doc.

3. Observation Tasks (ObsvTasks):
Data format is assumed to be a Pandas DataFrame of the structure
-index- | -Obsv_Task_ID- | -StripLocation_Long- | -StripLocation_Lat- | -Strip_Length- 

Assumptions:
    1.	Satellites in concern are Geo-Stationary Satellites – Orbiting over the Equator or in concentric circles with respect to Latitudes 
    for simplification of Distance/Time to the observation strip calculations.
    2.	Strip Location Parameter used in the script is considered to the west-most point of the strip, mid-point across width.
    3.	The influence of width of the Strip is neglected in computing Observation time.
    4.	Geographic Coordinate System (Latitude and Longitude) is used to represent the Location of the Satellite and the Observation Strip. 
    An added parameter of Altitude exists for the Satellite.
    5.	Earth velocity: 1670 kmph; Earth Radius: 6378.137 km. Distance units are in Kilometres and Time units are in Hours.
    6.	Each degree of Latitude is considered to be 110.567 km.
    7.	Setting Up of the satellite can be finished before starting observation.
"""
import pandas as pd
import math

def FIFO(DwInt, SatInfo, ObsvTasks):
    
    # Initialization
    
    # Creating an empty dataframe to hold the Plan for each satellites
    Plan_S = pd.DataFrame(columns = ["Task_ID", "Process_Type", "SetUp_Time" "Start_Time", "Processing_Time", "Energy_Status", "Data_Status"]) 
    # Process_Type defines weather the task is 'Observation' or 'Download'
    # Also a dictionary is created to keep dataframes for the satellites together
    SatID = SatInfo["Satellite_ID"].tolist() # Satellite IDs to be keys in the dictionary
    Sat_Schedule = {key: Plan_S for key in SatID} # Dictionary with empty dataframes for each satellites for Satellite Plans
    
    # Initializing Data and Energy Values to zero
    # d_status = d_start & e_status = e_start
    SatInfo["d_status"] = 0 # INITIALIZING AS ZERO AS SAID IN THE PAPER, MIGHT CAUSE ISSUES
    SatInfo["e_status"] = 0 # INITIALIZING AS ZERO AS SAID IN THE PAPER, MIGHT CAUSE ISSUES
    
    # Creating Universal Observation Time Windows from the Observation Tasks
    Earth_vel = 1670 # in kmph
    # Creating an empty dataframe to store schedules and associated paramters for each satellites
    SW_sched = pd.DataFrame(columns = ["Satellite_ID", "Obsv_Task_ID", "SetUp_Time", "Start_Time", "Processing_Time", "Roll_Angle"]) # Empty DataFrame to store the schedules of each satellite
    # Creating a Empty Dictionary to handle Observation Time Windows for each satellite
    # Satellite IDs to be keys in the dictionary
    TWO = {key: SW_sched for key in SatID} # Dictionary with empty dataframes for each satellites for Satellite schedules and paramters
    #  Nested loop for each Tasks from ObsvTasks and Corresponding Satellites from SatInfo to find Universal Observation Time windows for all satellites
    for indexS, rowS in SatInfo.iterrows(): # Loop over satellites list
        sat_ID = rowS["Satellite_ID"]
        sat_loc = [rowS["SatLocation_Long"], rowS["SatLocation_Lat"]] # Location for Satellite [Longitude, Latitude]
        sat_alt = rowS["Sat_Altitude"]
        sat_c1 = rowS["c1"]
        sat_c2 = rowS["c2"]    
        SWO = TWO[sat_ID] # Empty satellte schedules for observation           
        
        for indexT, rowT in ObsvTasks.iterrows(): # Loop over observation tasks
            task_ID = rowT["Obsv_Task_ID"]
            strip_loc = [rowT["StripLocation_Long"], rowT["StripLocation_Lat"]] # Location for Strip
            strip_len = rowT["Strip_Length"]
        
            # Finding the time at which observation starts and the processing time for each task by a satellite
            # Assuming time = 0 at the intial position given by sat_loc
            # Influencing factors:
            # 1. Earth's rotational velocity; 2. Relative Location of the Task and Satellite; 3. Setting up time for roll angle adjusment
            
            #  Distance between Satellite Location and the Strip Location and the required roll Angle
            distance, roll_angle = distance_and_roll(sat_loc[1], sat_loc[0], strip_loc[1], strip_loc[0], sat_alt)
            
            # Arrival time for the satellite at Strip beginning from initial position: 
            Start_time = distance/Earth_vel
            # Set-up time shall be set later after the order of execution is determined by sorting
            # since set-up time depends on the previous roll angle set up
            # Processing Time
            Process_time = strip_len/Earth_vel
            
            # Creating a list to append SWO DataFrame
            dat_list = [sat_ID, task_ID, 0, Start_time, Process_time, roll_angle] # Setp_Time set to zero initially
            SWO.loc[len(SWO)] = dat_list # Appending DataFrame
            
        # Sorting SWO to process the order of execution of tasks by a satellite to find set-up time
        SWO.sort_values("Start_Time")
        Init_Roll = 0 # Roll Angle would be zero before starting any obsrvation
        for indexSW, rowSW in SWO.iterrows(): # Loop over SWO
            Roll_Angle = rowSW["Roll_Angle"]
            SetUp_time = sat_c1 * abs(Init_Roll - Roll_Angle) + sat_c2 # Refer the paper
            Init_Roll = Roll_Angle # setting current roll angle as the previous roll angle for the following task
            SWO.at[indexSW, "SetUp_Time"] = SetUp_time # Assigned SetUp Time to the DataFrame
            
        # Removing Roll Angle from SWO to append SWO to TWO (Roll Angle no more required)
        del SWO["Roll_Angle"]
        # Sorting SWO in ascending order of Start_Time
        SWO.sort_values("Start_Time")
        
        # Updtating SWO in TWO dictionary for sat_ID
        TWO[sat_ID] = SWO
            
    # Main Loop
    # Loop over DataFrames in the dictionary TWO
    rowSWO = ((TWO_name, indexSW, rowSW) for TWO_name, SWOdict in TWO.items() for indexSW, rowSW in SWOdict.iterrows())
    time = 0
    for indexTW, rowTW in rowSWO: # Exits immediatly if observation plan for Satellite in concern is empty
        # Satellite ID
        satellite = rowTW["Satellite_ID"]
        t_prev = time
        time = rowTW["Start_Time"] # Starting time of observation for the satellite for the task
        # Cheking if there exists any data to be downloaded between begining time "t_prev" and starting observation at "time"
        # Finding the Dataframe for the Satellite with ID: satellite from the download schedule dictionary
        Plan_D = DwInt[satellite] # Download shcedule of the specific satellite from the download schedule dictionary
        # Filtering the DataFrame for the specific time - start - setup time >= t_prev and total time <= time
        filtered = Plan_D[(Plan_D["Download_Start_Time"] - Plan_D["Download_SetUp_Time"] >= t_prev) & (Plan_D["Download_Start_Time"] + Plan_D["Download_Processing_Time"] <= time)]
        # Start time - setup time is used previous task should end before setting up for new task
        
        # The Plan DataFrame for the current satellite from the Sat_Schedule dictionary
        Plan_S = Sat_Schedule[satellite]
        
        # If a Download Schedule exists 
        if not filtered.empty:
            p_type = "Download" # Process_Type for Plan_S
            
            # Keeping in mind that multiple download windows may exist in the timeframe, hence 'python lists' are used
            # filtered is of the structure -index- | -Dwd_Task_ID- | -Ground_Station_ID- | -Download_SetUp_Time- | -Download_Start_Time- | -Download_Processing_Time- 
            # for Plan_S we need -Task_ID- | -Process_Type- | -SetUp_Time- | -Start_Time- | -Processing_Time- | -Energy_Status- | -Data_Status-
            task_id = filtered["Dwd_Task_ID"].tolist()
            setup_time = filtered["Download_SetUp_Time"].tolist()
            start_time = filtered["Download_Start_Time"].tolist()
            process_time = filtered["Download_Processing_Time"].tolist()
            
            # For possible multiple download intervals
            for i in range(len(task_id)):
                # Updating data and energy status of the satellite // ENERGY CONSUMPTION FOR SETUP TIME NOT MENTIONED, HENCE AVOIDED
                # Current Data and Energy Status:
                # when t_prev == 0; status is read from SatInfo(initially), else last row of Plan_S
                if t_prev == 0:
                    cur_datasize = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "d_status"].values[0] # Current Data Size in satellite - Initial
                    cur_eglevel = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "d_status"].values[0] # Current Energy Level in satellite - Initial
                else:
                    # Last row of Plan_S; energy and data values at the end of the last operation
                    cur_datasize = Plan_S["Data_Status"].iloc[-1]
                    cur_eglevel = Plan_S["Energy_Status"].iloc[-1]
                # Data Update
                dw_datasize = 1 * process_time[i] # Downloaded Data Size, from paper
                data_update = cur_datasize - dw_datasize # updated Data Size in satellite, from Paper
                # Energy Update
                dw_egconsum = 0.1 * process_time[i] # energy consumption for download, from paper
                eg_gain = 0.1 * (setup_time[i] + process_time[i])   # Energy Gain from sun
                energy_update = cur_eglevel - dw_egconsum + eg_gain # updated Energy level in satellite, from Paper
                
                # Updating the satellite plan with all the data
                sched_dw = [task_id[i], p_type, setup_time[i], start_time[i], process_time[i], energy_update, data_update] # Dowload Schedule
                Plan_S.loc[len(Plan_S)] = sched_dw # Appending Plan_S with data from Plan_D
                
        # Assigning of observation task scheduling
        # Observation task starting at t = "time" from TWO to be merged into Plan_S
        p_type = "Observation" # Process_Type for Plan_S
        #  List of Data; data and energy updates to be added later after ensuring no conflict
        sched_obsv = [rowTW["Obsv_task_ID"], p_type, rowTW["SetUp_Time"], rowTW["Start_Time"], rowTW["Processing_Time"]] 
        
        # Updating data and energy status of the satellite
        # Data
        obsv_datasize = 1 * rowTW["Processing_Time"] # Data Gain
        cur_datasize = Plan_S["Data_Status"].iloc[-1] # Last row of Plan_S; data value at the end of the last operation
        data_update = cur_datasize + obsv_datasize
        # Energy
        obsv_egconsum = 1 * rowTW["Processing_Time"] # energy consumption for observation
        cur_eglevel = Plan_S["Energy_Status"].iloc[-1] # Last row of Plan_S; energy value at the end of the last operation  
        eg_gain = 0.1 * (rowTW["SetUp_Time"] + rowTW["Processing_Time"])
        energy_update = cur_eglevel - obsv_egconsum + eg_gain
        
        #  Append sched_obsv
        sched_obsv = sched_obsv + [energy_update, data_update]
        
        #  Checking for Data Conflict and Energy Conflict (Time Conflict omitted as it was and will be handled by the filtering for download task in the loop)
        #  d_size between d_min and d_max && e_sise between e_min and e_max
        d_min = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "d_min"].values[0]
        d_max = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "d_max"].values[0]
        e_min = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "e_min"].values[0]
        e_max = SatInfo.loc[SatInfo["Satellite_ID"] == satellite, "e_max"].values[0]
        # If loop:
        if ((data_update >= d_min and data_update <= d_max) and (energy_update >= e_min and energy_update <= e_max)):
            Plan_S.loc[len(Plan_S)] = sched_obsv # Updating Plan_S with observation task
            
        # Not deleting TWO elements since a For loop is made use of instead of While loop. will be deleted later.
        # Deleting the Observation Task added to Plan_S from the dataframes inside dictionart TWO for all the satellites to avoid
        # multiple satellites scheduling for the same task
        for delsched_name, delsched in TWO.itesm():
            if "Obsv_task_ID" in delsched.columns:
                TWO[delsched_name] = delsched[delsched["Obsv_task_ID"] != rowTW["Obsv_task_ID"]]
        
        # Updating Plan_S in the Dictionary: Sat_Schedule
        Sat_Schedule[satellite] = Plan_S
        
    # Adding a download task after the last observation task: for each satellite
    # Looping through Sat_Schedule dictionary:
    for sat_id in Sat_Schedule:
        Plan_S = Sat_Schedule[sat_id] # Plan_S for satellite with ID "sat_id"
        Plan_D = DwInt[sat_id] # Download shcedule of the specific satellite from the dictionary
        
        # Checking for any download tasks after the last observation task for Satellite: "sat_id"
        # Last task and ending time:
        e_starttime = Plan_S["Start_Time"].iloc[-1]
        e_processtime = Plan_S["Processing_Time"].iloc[-1]
        end_time = e_starttime + e_processtime # time at which last task ends
        # Filtering Plan_D DataFrame for download tasks after end_time
        filter_end = Plan_D[(Plan_D["Download_Start_Time"] - Plan_D["Download_SetUp_Time"] >= end_time)]
        
        if not filter_end.empty:
            p_type = "Download" # Process_Type for Plan_S
            
            # Keeping in mind that multiple download windows may exist in the timeframe, hence 'python lists' are used
            # filtered is of the structure -index- | -Dwd_Task_ID- | -Ground_Station_ID- | -Download_SetUp_Time- | -Download_Start_Time- | -Download_Processing_Time- 
            # for Plan_S we need -Task_ID- | -Process_Type- | -SetUp_Time- | -Start_Time- | -Processing_Time- | -Energy_Status- | -Data_Status-
            task_id = filter_end["Dwd_Task_ID"].tolist()
            setup_time = filter_end["Download_SetUp_Time"].tolist()
            start_time = filter_end["Download_Start_Time"].tolist()
            process_time = filter_end["Download_Processing_Time"].tolist()
            
            # For possible multiple download intervals
            for i in range(len(task_id)):
                # Updating data and energy status of the satellite // ENERGY CONSUMPTION FOR SETUP TIME NOT MENTIONED, HENCE AVOIDED
                # Current Data and Energy Status:
                # Last row of Plan_S; energy and data values at the end of the last operation
                cur_datasize = Plan_S["Data_Status"].iloc[-1]
                cur_eglevel = Plan_S["Energy_Status"].iloc[-1]
                # Data Update
                dw_datasize = 1 * process_time[i] # Downloaded Data Size, from paper
                data_update = cur_datasize - dw_datasize # updated Data Size in satellite, from Paper
                # Energy Update
                dw_egconsum = 0.1 * process_time[i] # energy consumption for download, from paper
                eg_gain = 0.1 * (setup_time[i] + process_time[i])   # Energy Gain from sun
                energy_update = cur_eglevel - dw_egconsum + eg_gain # updated Energy level in satellite, from Paper
                
                # Updating the satellite plan with all the data
                e_sched_dw = [task_id[i], p_type, setup_time[i], start_time[i], process_time[i], energy_update, data_update] # Dowload Schedule
                Plan_S.loc[len(Plan_S)] = e_sched_dw # Appending Plan_S with data from Plan_D
                
        # Updating Sat_Schdule with updated Plan_S
        Sat_Schedule[satellite] = Plan_S
        
    # Delete TWO
    del TWO
    # Retruning the dictionary of schedules of all satellites in the constellation
    return Sat_Schedule

# MAIN FUNCTION ENDS HERE


def distance_and_roll(lat_sat, lon_sat, lat_pos, lon_pos, altitude):

    # Calculate the great-circle distance between two Nadir of satellite and observation point on the Earth surface.
    radius = 6378.137 # in kilometers
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat_sat, lon_sat, lat_pos, lon_pos])

    # Compute differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Haversine Distance in kilometers
    havr_distance = radius * c # considers earths curvature
    
    # Caluclating perpendicular distance between satellite orbit and observation point in kilometers
    # Approximating 1 degree of latitude is 110.537km while close to equator
    perp_distance = (lat_pos - lat_sat) * 110.537 # considers earths curvature
    
    # Using Spherical Pythagorean theorem to find the distance between satellite nadir point and the 
    # line perpendicular to satellite orbit line passing through observation point
    # conversion to angular distances:
    ang_havr = (havr_distance/radius)
    ang_perp = (perp_distance/radius)
    
    # Applying spherical pythagorean theorem
    cos_ang_dist = math.cos(ang_havr) * math.cos(ang_perp)
    ang_dist = math.acos(cos_ang_dist)
    # Converting to linear distance
    earth_distance = radius * ang_dist # Distance on the surface of the earth
    
    # Distance to be covered by satellite:
    # Considering 'earth_distance' as an arc length and the distance to be covered by satellite would be
    # another arc considering the earth and satellite orbit to be concentric circles.
    # Hence the Arc Angle would be the same: finding arc angle for 'earth_distance'
    theta_arc = (earth_distance * 180)/(math.pi * radius)
    distance = theta_arc * (math.pi/180) * (radius + altitude) # distance to be travelled by satellite

    # Compute the roll angle required for the satellite to observe the observation point.
    # Using Pythagorean theorem. Perpendiular distance to be assumed without curve since the length is minimal to be affected by curvature.
    roll_angle_rad = math.atan(perp_distance/altitude)
    roll_angle = math.degrees(roll_angle_rad) # roll angle required by Satellite
    
    return distance, roll_angle            
