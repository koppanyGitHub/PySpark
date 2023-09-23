### Running locally 
Execute the commands inside the `Task1` directory

Run `pylint Task1`

`TSV_PATH=./data pytest -c pytest.ini`

`TSV_PATH=./data python task1.py`

### Building the image and running it
Build the image
`docker build -t task1 .`

Run a container that removes itself after completion
`docker run --rm -v ./data:/app/data --name task1c task1`

To enter into the container use
`docker exec -it task1c /bin/bash`



 

