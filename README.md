# Geoclusterizer
Clustering routine that uses publicly-available data to group like geographical areas

Marketing professionals often use customer segmentation to serve more differentiated products and services to their customers. Traditionally, marketing analysts have used a set of heuristics and Excel-based analytical methods to segment customers. More recently, unsupervised learning techniques have been employed to segment / cluster customers at scale.

The Geoclusterizer groups like geographical areas on the basis of publicly-available demographic data. We reduce the dimensionality of the dataset using the [LinearCorex](https://github.com/gregversteeg/LinearCorex) method and then fitting a [Gaussian Mixture Model](https://scikit-learn.org/stable/modules/mixture.html#) on that data to generate clusters of Census tracts.

# Getting Started

Software dependencies: Install Anaconda and create a virtual environment using the environment.yml file.

Reproduce the results:
* Geoclusterizer uses the pydoit dependency management framework to run tasks; however, the user can also run each task as a standalone py file
* The software is organized into the following tasks, which are listed in the ```dodo.py``` file
  * makedirs: Make directories if they don't exist 
  * download_acs: [Download American Community Survey (ACS) data](https://www.census.gov/programs-surveys/acs/data.html)
  * parse_acs: Parse downloaded ACS data into standalone tables
  * scale_and_impute_data: Scale dataset and impute missing data
  * select_n_components: Select number of components to use
  * train_model: Train 
* To run all the tasks at once, simply cd into the repository and, if all packages have been installed correctly, type ```doit```
* Outputs are saved in the data/processed directory, which is created by one of the tasks
