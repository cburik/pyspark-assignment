from distutils.core import setup

setup(
    name='pyspark_assignment',
    py_modules=['pyspark_assignment'],
    package_data={'example_input': ['input/*.csv'],
                  'example_output': ['client_data/example.csv']},
    version='1.0.0',
    description='A PySpark Assignment merging and filtering two DataFrames',
    install_requires=['pyspark>=3']
)
