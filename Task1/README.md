### Running locally 
Run `pylint Task1`

`TSV_PATH=./data pytest -c pytest.ini`

`TSV_PATH=./data python task1.py`

### Building the image and running it
`docker build -t task1 .`

`docker run -v data:/app/data task1 /bin/sh -c "pytest && pylint Task1"`

 

