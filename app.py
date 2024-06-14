import streamlit as st
from py2insertvalue import*
from py2getvalues import*

tab1, tab2, tab3 = st.tabs(["Data Harvesting / Warehousing", "Channel Info", "Pre-defined Reports"])




with tab1:
    st.header("Data Harvesting / Warehousing")
    channelid = st.text_input("Input Channel ID and Press Submit", "")
    

    if st.button("Submit"):
        st.info('Please wait until data is stored', icon="ℹ️")

        with st.status("Extracting data and storing in SQL...", expanded=True) as status:
            st.write("Extracting Channel Data...")
            extract_channelData(channelid)
            st.write("Extracting Video Data...")
            extract_videocommentData(channelid)
            st.write("Extracting Playlist Data...")
            extract_playlistData(channelid)
            status.update(label="Download complete!", state="complete", expanded=False)

        st.success('Data has beensuccessfully stored to SQL', icon="✅")


    
with tab2:
    st.header("Channel Info")
    option = st.selectbox('Select Channel Name..',(getChannelname()),index=None,placeholder="Select Channel Name...")


    if st.button('Submit '): 
        st.write((channelname(option)))



with tab3:
    st.header("Pre-Defined Reports")
    Q1 = '1.What are the names of all the videos and their corresponding channels?'
    Q2 = '2.Which channels have the most number of videos, and how many videos do they have?'
    Q3 = '3.What are the top 10 most viewed videos and their respective channels?'
    Q4 = '4.How many comments were made on each video, and what are their corresponding video names?'
    Q5 = '5.Which videos have the highest number of likes, and what are their corresponding channel names?'
    Q6 = '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
    Q7 = '7.What is the total number of views for each channel, and what are their corresponding channel names?'
    Q8 = '8.What are the names of all the channels that have published videos in the year 2022?'
    Q9 = '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    Q10 = '10.Which videos have the highest number of comments, and what are their corresponding channel names?'

    option = st.selectbox("Select the Question below", (Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10),
                         index=None,placeholder="Select Question...")
    

    if option == Q1:
        st.write(Query1())

    if option == Q2:
        st.write(Query2())
    
    if option == Q3:
        st.write(Query3())
    
    if option == Q4:
        st.write(Query4())

    if option == Q5:
        st.write(Query5())

    if option == Q6:
        st.write(Query6())

    if option == Q7:
        st.write(Query7())
    
    if option == Q8:
        st.write(Query8())

    if option == Q9:
        st.write(Query9())

    if option == Q10:
        st.write(Query10())







#streamlit run c:/Users/ashfaq.ahamed/Documents/projects1/testing/app.py