A take home test for Coda Metrix

# Installation Instructions

## If you already have spark v3.5 installed
1. Install python venv using Python version 3.12
```
python312 -m venv venv
```
2. Install dependencies
```
.\venv\Scripts\activate
pip install -r requirements.txt
```

3. Install the project
```
pip install -e .
```
4. Run the tests
```
pytest
```

## Alternatively
Docker may be used to run this independently.

1. Build the docker image
```
Docker build . -t batmanq96/codametrix
```

2. Run the container
```
Docker run -it batmanq96/codametrix bash
```

## Another alternative
You can pull the already built container
1. Pull from dockerhub
```
Docker pull batmanq96/codametrix
```

2. Run the tests as above
```
Docker run -it batmanq96/codametrix bash
```