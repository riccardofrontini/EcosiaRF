# Ecosia Data Engineering Assignment
<b> Riccardo Frontini - July 2021 </b>

The following example represents the first two elements contained in the JSON file:

```{
    "data": [
        {
            "category": "Sports",
            "event_name": "View Project",
            "gender": "M",
            "age": "18-24",
            "marital_status": "married",
            "session_id": "69f62d2ae87640f5a2dde2b2e9229fe6",
            "device": "android",
            "client_time": 1393632004,
            "location": {
                "latitude": 40.189788,
                "city": "Lyons",
                "state": "CO",
                "longitude": -105.35528,
                "zip_code": "80540"
            }
        },
        {
            "category": "Technology",
            "event_name": "View Project",
            "gender": "M",
            "age": "18-24",
            "marital_status": "single",
            "session_id": "4459d001feb8438eae5f4ec24abcd992",
            "device": "iOS",
            "client_time": 1393632022,
            "location": {
                "latitude": 33.844371,
                "city": "Alpharetta",
                "state": "GA",
                "longitude": -84.47405,
                "zip_code": "30009"
            }
        }
    ]
}```

Records appear to be encapsulated as elements in a `data` array: when parsed we obtain a single column data-frame as shown below


```python
import pandas as pd
```


```python
rawJSONurl = 'https://raw.githubusercontent.com/localytics/data-viz-challenge/master/data.json'
raw_df = pd.read_json(rawJSONurl)
raw_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>data</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{'category': 'Sports', 'event_name': 'View Pro...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>{'category': 'Technology', 'event_name': 'View...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{'category': 'Environment', 'event_name': 'Vie...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>{'category': 'Technology', 'event_name': 'View...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>{'category': 'Sports', 'event_name': 'View Pro...</td>
    </tr>
  </tbody>
</table>
</div>



We are offered two strategies to deal with this format: using the `pd.read_json()` method with an `orient=split` argument, or using the `pd.json_normalize()` method


```python
pd.read_json(rawJSONurl, orient='split')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>event_name</th>
      <th>gender</th>
      <th>age</th>
      <th>marital_status</th>
      <th>session_id</th>
      <th>device</th>
      <th>client_time</th>
      <th>location</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>married</td>
      <td>69f62d2ae87640f5a2dde2b2e9229fe6</td>
      <td>android</td>
      <td>2014-03-01 00:00:04</td>
      <td>{'latitude': 40.189788, 'city': 'Lyons', 'stat...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>4459d001feb8438eae5f4ec24abcd992</td>
      <td>iOS</td>
      <td>2014-03-01 00:00:22</td>
      <td>{'latitude': 33.844371, 'city': 'Alpharetta', ...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Environment</td>
      <td>View Project</td>
      <td>M</td>
      <td>55+</td>
      <td>single</td>
      <td>0db9ed700a184d48a9d04806696e3642</td>
      <td>iOS</td>
      <td>2014-03-01 00:00:32</td>
      <td>{'latitude': 42.446396, 'city': 'Westford', 's...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>68195e2372bd4022b17220fc21de9138</td>
      <td>android</td>
      <td>2014-03-01 00:00:38</td>
      <td>{'latitude': 44.624413, 'city': 'Saranac', 'st...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Sports</td>
      <td>View Project</td>
      <td>F</td>
      <td>25-34</td>
      <td>married</td>
      <td>9508a8385dc94773baba8aa7d1c2aa75</td>
      <td>iOS</td>
      <td>2014-03-01 00:00:51</td>
      <td>{'latitude': 36.747083, 'city': 'Lampe', 'stat...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>49995</th>
      <td>Sports</td>
      <td>Fund Project</td>
      <td>F</td>
      <td>18-24</td>
      <td>married</td>
      <td>412b973788704c36a008a506cdbba033</td>
      <td>iOS</td>
      <td>2014-03-31 23:48:40</td>
      <td>{'latitude': 45.343615, 'city': 'West Linn', '...</td>
      <td>35.0</td>
    </tr>
    <tr>
      <th>49996</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>single</td>
      <td>4addd36a6f4347c59865fe04a92e8d57</td>
      <td>android</td>
      <td>2014-03-31 23:49:29</td>
      <td>{'latitude': 41.079983, 'city': 'Greenwich', '...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>49997</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>25-34</td>
      <td>single</td>
      <td>3181a138a0b94d1da9a3d29f7816fcc7</td>
      <td>iOS</td>
      <td>2014-03-31 23:51:33</td>
      <td>{'latitude': 40.36502, 'city': 'Irwin', 'state...</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>49998</th>
      <td>Sports</td>
      <td>Fund Project</td>
      <td>F</td>
      <td>35-44</td>
      <td>married</td>
      <td>2df6b9a0c66b48a389330327517b9276</td>
      <td>iOS</td>
      <td>2014-03-31 23:52:48</td>
      <td>{'latitude': 39.477625, 'city': 'Martinsville'...</td>
      <td>37.0</td>
    </tr>
    <tr>
      <th>49999</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>fc24339bfda84ff29102981cbee78023</td>
      <td>iOS</td>
      <td>2014-03-31 23:58:45</td>
      <td>{'latitude': 45.408374, 'city': 'Lake Oswego',...</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>50000 rows × 10 columns</p>
</div>



This still leaves us with the nested `location` struct, which would require some post-processing for flattening.


```python
#import needed libraries to access the raw data
import json
from urllib.request import Request, urlopen

#access the remote URL
req = Request(rawJSONurl)
response = urlopen(req)

#parse data and normalize into a pandas dataframe
jsonData = json.loads(response.read().decode('utf-8'))
normalized_df = pd.json_normalize(jsonData['data'])
normalized_df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>event_name</th>
      <th>gender</th>
      <th>age</th>
      <th>marital_status</th>
      <th>session_id</th>
      <th>device</th>
      <th>client_time</th>
      <th>location.latitude</th>
      <th>location.city</th>
      <th>location.state</th>
      <th>location.longitude</th>
      <th>location.zip_code</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>married</td>
      <td>69f62d2ae87640f5a2dde2b2e9229fe6</td>
      <td>android</td>
      <td>1393632004</td>
      <td>40.189788</td>
      <td>Lyons</td>
      <td>CO</td>
      <td>-105.355280</td>
      <td>80540</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>4459d001feb8438eae5f4ec24abcd992</td>
      <td>iOS</td>
      <td>1393632022</td>
      <td>33.844371</td>
      <td>Alpharetta</td>
      <td>GA</td>
      <td>-84.474050</td>
      <td>30009</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Environment</td>
      <td>View Project</td>
      <td>M</td>
      <td>55+</td>
      <td>single</td>
      <td>0db9ed700a184d48a9d04806696e3642</td>
      <td>iOS</td>
      <td>1393632032</td>
      <td>42.446396</td>
      <td>Westford</td>
      <td>MA</td>
      <td>-71.459405</td>
      <td>01886</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>68195e2372bd4022b17220fc21de9138</td>
      <td>android</td>
      <td>1393632038</td>
      <td>44.624413</td>
      <td>Saranac</td>
      <td>NY</td>
      <td>-73.809266</td>
      <td>12981</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Sports</td>
      <td>View Project</td>
      <td>F</td>
      <td>25-34</td>
      <td>married</td>
      <td>9508a8385dc94773baba8aa7d1c2aa75</td>
      <td>iOS</td>
      <td>1393632051</td>
      <td>36.747083</td>
      <td>Lampe</td>
      <td>MO</td>
      <td>-93.458626</td>
      <td>65681</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>married</td>
      <td>d420e0e6927c4bebaa580e99b00e52e9</td>
      <td>iOS</td>
      <td>1393632064</td>
      <td>41.472250</td>
      <td>Cleveland</td>
      <td>OH</td>
      <td>-81.740305</td>
      <td>44102</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Games</td>
      <td>View Project</td>
      <td>F</td>
      <td>35-44</td>
      <td>single</td>
      <td>0e00548eb6a54d2f8dbe2bdf6c8efb80</td>
      <td>iOS</td>
      <td>1393632137</td>
      <td>40.719240</td>
      <td>Middle Village</td>
      <td>NY</td>
      <td>-73.892791</td>
      <td>11379</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>F</td>
      <td>35-44</td>
      <td>married</td>
      <td>c6f18d84b43b4a4a90fa9a44016c3665</td>
      <td>android</td>
      <td>1393632138</td>
      <td>37.103768</td>
      <td>Los Banos</td>
      <td>CA</td>
      <td>-120.847479</td>
      <td>93635</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>married</td>
      <td>2eb996fba97548b88f8ea5ec2484b34b</td>
      <td>iOS</td>
      <td>1393632145</td>
      <td>41.040988</td>
      <td>Rochester</td>
      <td>IN</td>
      <td>-86.254272</td>
      <td>46975</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>married</td>
      <td>e040fa23c3f84ad58ca59f1552fa3f0b</td>
      <td>iOS</td>
      <td>1393632159</td>
      <td>40.203459</td>
      <td>Newtown</td>
      <td>IN</td>
      <td>-87.147014</td>
      <td>47969</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>a0d400c1626047c29060259d08ee0385</td>
      <td>android</td>
      <td>1393632160</td>
      <td>40.189788</td>
      <td>Lyons</td>
      <td>CO</td>
      <td>-105.355280</td>
      <td>80540</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Fashion</td>
      <td>Fund Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>married</td>
      <td>2eb996fba97548b88f8ea5ec2484b34b</td>
      <td>iOS</td>
      <td>1393632162</td>
      <td>41.040988</td>
      <td>Rochester</td>
      <td>IN</td>
      <td>-86.254272</td>
      <td>46975</td>
      <td>61.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Sports</td>
      <td>View Project</td>
      <td>F</td>
      <td>18-24</td>
      <td>married</td>
      <td>a1fdfe0bebed4510a9059bcfb3ba1325</td>
      <td>iOS</td>
      <td>1393632171</td>
      <td>33.794055</td>
      <td>Atlanta</td>
      <td>GA</td>
      <td>-84.377326</td>
      <td>30308</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>45-54</td>
      <td>single</td>
      <td>efe1feab12f54efd8edc7493cf692b13</td>
      <td>android</td>
      <td>1393632184</td>
      <td>36.106464</td>
      <td>Henderson</td>
      <td>NV</td>
      <td>-114.919174</td>
      <td>89011</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>35-44</td>
      <td>single</td>
      <td>0fc792e49de847fb8b954a5bdcfaf213</td>
      <td>iOS</td>
      <td>1393632188</td>
      <td>37.448044</td>
      <td>Pittsburg</td>
      <td>KS</td>
      <td>-94.819212</td>
      <td>66762</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Technology</td>
      <td>View Project</td>
      <td>F</td>
      <td>18-24</td>
      <td>single</td>
      <td>1b2a6e9a2e1f46b8827db37a28e7196e</td>
      <td>iOS</td>
      <td>1393632199</td>
      <td>33.724907</td>
      <td>Atlanta</td>
      <td>GA</td>
      <td>-84.468329</td>
      <td>30311</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Sports</td>
      <td>Fund Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>married</td>
      <td>69f62d2ae87640f5a2dde2b2e9229fe6</td>
      <td>android</td>
      <td>1393632200</td>
      <td>40.189788</td>
      <td>Lyons</td>
      <td>CO</td>
      <td>-105.355280</td>
      <td>80540</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Games</td>
      <td>View Project</td>
      <td>F</td>
      <td>18-24</td>
      <td>married</td>
      <td>05f9b091b72e488e82a42bd373aae337</td>
      <td>iOS</td>
      <td>1393632211</td>
      <td>33.844371</td>
      <td>Alpharetta</td>
      <td>GA</td>
      <td>-84.474050</td>
      <td>30009</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Technology</td>
      <td>Fund Project</td>
      <td>M</td>
      <td>18-24</td>
      <td>single</td>
      <td>a0d400c1626047c29060259d08ee0385</td>
      <td>android</td>
      <td>1393632211</td>
      <td>40.189788</td>
      <td>Lyons</td>
      <td>CO</td>
      <td>-105.355280</td>
      <td>80540</td>
      <td>39.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Fashion</td>
      <td>View Project</td>
      <td>F</td>
      <td>25-34</td>
      <td>married</td>
      <td>84602ee2b44e4e91967117cfca57fa89</td>
      <td>iOS</td>
      <td>1393632213</td>
      <td>43.775697</td>
      <td>Rosendale</td>
      <td>WI</td>
      <td>-88.659504</td>
      <td>54974</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Though less straightforward when it comes to accessing the remote data, the `json_normalize()` method already gives us a fully flat structure for the records, normalizing the `location` struct into its components

Once the data structure is adequate for our needs, we can move on to filtering only the needed data and projecting only the columns that are going to be useful for later steps.
We also keep `gender` and `location.state` for a visual confirmation of the filtering: those will be dropped at a later stage.


```python
filtered_df = normalized_df[(normalized_df["gender"]=="F") & \
                     (normalized_df['location.state']=="CA")] \
        [["device", "age", "client_time", "amount", 'gender', 'location.state']]
filtered_df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>device</th>
      <th>age</th>
      <th>client_time</th>
      <th>amount</th>
      <th>gender</th>
      <th>location.state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>7</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393632138</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>68</th>
      <td>android</td>
      <td>55+</td>
      <td>1393632938</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>71</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393633038</td>
      <td>33.0</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>72</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393633048</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>90</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393633344</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>97</th>
      <td>android</td>
      <td>18-24</td>
      <td>1393633432</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>179</th>
      <td>android</td>
      <td>25-34</td>
      <td>1393634494</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>255</th>
      <td>iOS</td>
      <td>35-44</td>
      <td>1393635667</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>353</th>
      <td>iOS</td>
      <td>25-34</td>
      <td>1393637079</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>494</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393639621</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>590</th>
      <td>android</td>
      <td>18-24</td>
      <td>1393641725</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>598</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393641891</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>600</th>
      <td>android</td>
      <td>25-34</td>
      <td>1393641912</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>624</th>
      <td>iOS</td>
      <td>55+</td>
      <td>1393642390</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>703</th>
      <td>iOS</td>
      <td>55+</td>
      <td>1393644057</td>
      <td>9.0</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>712</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393644262</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>857</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393647112</td>
      <td>22.0</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>863</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393647272</td>
      <td>42.0</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>908</th>
      <td>android</td>
      <td>55+</td>
      <td>1393648214</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>913</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393648387</td>
      <td>NaN</td>
      <td>F</td>
      <td>CA</td>
    </tr>
  </tbody>
</table>
</div>



What's missing at this point is a `date` field: the provided `client_time` represents a timestamp in the EPOCH format, which we need to parse and truncate to the day. The `dt.date` already gives us the desired date format ( `YYYY-MM-DD` )
In order to correctly aggregate the data, we substitute the `NaN` values in the `amount` field with zeros.


```python
filtered_df['date'] = pd.to_datetime(filtered_df["client_time"], unit='s').dt.date
filtered_df['amount'] = filtered_df['amount'].fillna(0)
filtered_df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>device</th>
      <th>age</th>
      <th>client_time</th>
      <th>amount</th>
      <th>gender</th>
      <th>location.state</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>7</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393632138</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>68</th>
      <td>android</td>
      <td>55+</td>
      <td>1393632938</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>71</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393633038</td>
      <td>33.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>72</th>
      <td>android</td>
      <td>35-44</td>
      <td>1393633048</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>90</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393633344</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>97</th>
      <td>android</td>
      <td>18-24</td>
      <td>1393633432</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>179</th>
      <td>android</td>
      <td>25-34</td>
      <td>1393634494</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>255</th>
      <td>iOS</td>
      <td>35-44</td>
      <td>1393635667</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>353</th>
      <td>iOS</td>
      <td>25-34</td>
      <td>1393637079</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>494</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393639621</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>590</th>
      <td>android</td>
      <td>18-24</td>
      <td>1393641725</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>598</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393641891</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>600</th>
      <td>android</td>
      <td>25-34</td>
      <td>1393641912</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>624</th>
      <td>iOS</td>
      <td>55+</td>
      <td>1393642390</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>703</th>
      <td>iOS</td>
      <td>55+</td>
      <td>1393644057</td>
      <td>9.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>712</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393644262</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>857</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393647112</td>
      <td>22.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>863</th>
      <td>iOS</td>
      <td>18-24</td>
      <td>1393647272</td>
      <td>42.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>908</th>
      <td>android</td>
      <td>55+</td>
      <td>1393648214</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
    <tr>
      <th>913</th>
      <td>android</td>
      <td>45-54</td>
      <td>1393648387</td>
      <td>0.0</td>
      <td>F</td>
      <td>CA</td>
      <td>2014-03-01</td>
    </tr>
  </tbody>
</table>
</div>



At this stage, we can aggregate the data into the desired format.


```python
aggregations = {
    'client_time': 'count',
    'amount': 'sum'
}

aggregate_df = filtered_df.groupby(['age', 'device', 'date'], as_index=False).agg(aggregations).rename(columns={
    "client_time": "count", "amount": "amount_sum"
})
aggregate_df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>age</th>
      <th>device</th>
      <th>date</th>
      <th>count</th>
      <th>amount_sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-01</td>
      <td>13</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-02</td>
      <td>5</td>
      <td>69.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-03</td>
      <td>6</td>
      <td>40.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-04</td>
      <td>7</td>
      <td>49.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-05</td>
      <td>5</td>
      <td>113.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-06</td>
      <td>2</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-07</td>
      <td>8</td>
      <td>116.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-08</td>
      <td>9</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-09</td>
      <td>1</td>
      <td>27.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-10</td>
      <td>9</td>
      <td>105.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-11</td>
      <td>4</td>
      <td>45.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-12</td>
      <td>6</td>
      <td>200.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-13</td>
      <td>11</td>
      <td>142.0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-14</td>
      <td>4</td>
      <td>85.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-15</td>
      <td>3</td>
      <td>112.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-16</td>
      <td>3</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-17</td>
      <td>4</td>
      <td>57.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-18</td>
      <td>5</td>
      <td>48.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-19</td>
      <td>5</td>
      <td>93.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>18-24</td>
      <td>android</td>
      <td>2014-03-20</td>
      <td>5</td>
      <td>177.0</td>
    </tr>
  </tbody>
</table>
</div>



Finally, we can serialize the desired output as a CSV on S3: we can leverage `pandas` in conjunction with `s3fs` to produce the file and upload it.
In order to do this we need a variable containing the bucket name, and the variables containing the Key ID and the Key Secret.


```python
AWS_S3_BUCKET = 'my-bucket-name'
AWS_ACCESS_KEY_ID = 'MYKEYID'
AWS_SECRET_ACCESS_KEY = 'MYSECRETKEY'

opt = {
    'key' : AWS_ACCESS_KEY_ID,
    'secret' : AWS_SECRET_ACCESS_KEY
}

aggregate_df.to_csv(f"s3://{AWS_S3_BUCKET}/csv/total_events.csv", storage_options=opt, index=False)
```
