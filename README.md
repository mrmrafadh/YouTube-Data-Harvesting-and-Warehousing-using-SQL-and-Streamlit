# YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit
Collect Data from different YouTube channels using Google Client API and analyze data

We need to make an app using Streamlit. This app will let users look at and study data from many YouTube channels. Here’s what the app should do:

This process involves a series of steps to extract, transform, and visualize data from YouTube channels using various tools and technologies.

Data Extraction: The Google API client library for Python is used to connect to the YouTube API. This allows users to fetch data such as channels, videos, and comments by providing the Channel ID.

Data Storage: The retrieved data from the YouTube API is then stored in a DataFrame for further processing.

Data Transformation: The data collected from multiple channels is transformed and migrated to a structured MySQL data warehouse. This structured format facilitates efficient data management and querying.

Data Retrieval: SQL queries are used to join tables in the data warehouse and fetch data for specific channels based on user input. This provides interesting insights about the YouTube channels.

Data Visualization: The retrieved data is then displayed in a Streamlit app. Plotly’s data visualization features are used to create charts and graphs, enabling users to analyze the data effectively.

In summary, this approach involves building a user-friendly interface with Streamlit, extracting data from the YouTube API, storing it in a DataFrame, transforming and migrating it to a SQL data warehouse, retrieving specific data using SQL queries, and visualizing the data in the Streamlit app.


![youtube data extractor](https://github.com/mrmrafadh/YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit/assets/167102646/8af9a367-d68a-4812-a1f9-874117b10a09)

Upon launching the application, the user is presented with a list of channels that have been previously stored in the database. The application provides a ‘Clear Database’ button, which allows the user to erase all previously saved channels.

The ‘Grab Data’ button is designed to fetch data corresponding to the channel ID entered by the user in the provided text box. The user has the capacity to fetch data from up to 10 channels.

Once the data is collected, it can be transferred to a MySQL database by clicking the ‘Save To Analyse Data’ button. The application also features a drop-down list populated with various analytical questions. By selecting a question from this list, the user can obtain an appropriate answer derived from the collected data.

Also this application has the capablity of error handle such as:
1) preventing user from entering duplicate channels
2) capable to validate if the channel id
