# ESG AI
**Demonstrating the power of Streamlit.** [![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/hannahawalsh/esg_ai/main/app.py)

## Background
Environmental, Social, & Governance (ESG) investing has become the latest rage in the world of finance. The idea is to invest in companies that are sustainable, particularly in in the 3 ESG categories:
<ul>
<li><b>Environmental</b> - Issues such as climate change and pollution</li>
<li><b>Social</b> - Issues around workplace practices and human capital</li>
<li><b>Governance</b> - Issues such as executive pay, accounting, and ethics</li>
</ul>

There has been a tremendous amount of research around ESG investing. Harvard Law School Forum on Corporate Governance published a paper titled "ESG Matters" in which they studied companies with particularly high ESG scores compared to those with low scores with the following conclusions: 
<ol>
<li>Higher ESG is associated with higher profitability and lower volatility</li>
<li>High ESG scoring companies tend to be good allocators of capital</li>
<li>Good ESG companies generally have higher valuations, EVA growth, size, and returns</li>
</ol>

### ESG Reporting 
Currently 90% of S&P 500 companies publish annual sustainability reports, which can range from as little as 30 pages to over 200 pages. There is not one clear reporting format, but there are some general reporting guidelines (e.g Nasdaq). Analysts leverage these reports to understand company trends and themes, which can take weeks for an investment thesis on a particular company. 

### Greenwashing 
Greenwashing is the practice of making statements or policies that make an investment appear more serious about ESG than it actually is. We need to be mindful of ESG reporting and make sure we are leveraging credible sources to minimize the effect of greenwashing on our sustainability analysis.

### Current Approach
ESG scoring is tricky. Research analysts leverage many sources to manually come up with scores around the ESG categories. These scores are updated every so often and are not real-time. As there are thousands of companies, the current approach is hardly scalable. 

### Our Approach
We aim to make ESG scoring a data driven approach. We leverage the GDelt news source to ingest historical and real-time news articles that we classify into the ESG categories. We then perform scoring based on sentiment, which can be adjusted based on given windows of time. Additionally, we leverage deep learning to embed the connections on a graph from news article mentions. This allows us to find better suggested competitors to compare ESG results against.

### Examples  of ESG found in News

#### E: Nike (NKE): 
“Its Flyknit and Flyleather products were developed with environmental sustainability in mind. Nike signed onto a coalition of companies called RE100, vowing to source 100% renewable energy across its operations by 2025. There's more, but any interested investors should read Nike's latest sustainability report, which uses the GRI framework, the Sustainability Accounting Standards Board (SASB), and the United Nations' Sustainable Development Goals (SDG).”

#### S: Accenture (ACN): 
“Accenture pays close attention to its diversity and inclusion in its workforce. The company plans to improve its workplace gender ratios, with a goal to have 50% female and 50% male employees by the end of 2025. Accenture plans to better its corporate makeup as well, pledging to have at least 25% female managing directors by 2020.”

#### G: Intuit (INTU):
“It has achieved a 40% diverse board, one of the highest levels in corporate America today. Intuit shows accountability by tying its executives' incentive compensation to revenue and non-GAAP (Generally Accepted Accounting Principles) operating income, as well as to the company's overall performance on annual goals related to employees, customers, partners, and stockholders.”

## App Installation

We have packaged our application as a streamlit app. Note that a majority of the codebase is performed in databricks and an architecture diagram can be found in our slides. However, we have created an application to run locally. To get started, clone the repository into a directory of your choice and ensure your python environment has the following dependencies:
<ul>
<li>Streamlit</li> (version 0.70.0 or later)
<li>Pandas</li> (version 1.0.0 or later)
<li>Numpy</li>
<li>Altair</li>
<li>NetworkX</li>
<li>Plotly</li>
<li>Colour</li>
</ul>

Navigate to the directory you cloned and run the following in your command prompt:
```bash
streamlit run app.py
```
You should be navigated to a localhost link where you will see the application running.

Thanks to Streamlit Sharing, we have also been able to host the app for free! You can find that [**HERE**](https://share.streamlit.io/hannahawalsh/esg_ai/main/app.py)!

## License
[MIT](https://choosealicense.com/licenses/mit/)
