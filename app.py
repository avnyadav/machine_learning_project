from multiprocessing.dummy import Pipe
from xml.etree.ElementTree import PI
from flask import Flask,request
import sys

from matplotlib.style import context
from housing.logger import logging
from housing.exception import HousingException
import os,sys
from housing.pipeline.pipeline import Pipeline
from housing.entity.housing_predictor import HousingPredictor,HousingData
from flask import send_file, abort, render_template
ROOT_DIR = os.getcwd()
LOG_FOLDER_NAME = "housing_logs"
PIPELINE_FOLDER_NAME = "housing"
SAVED_MODELS_DIR_NAME = "saved_models"
LOG_DIR = os.path.join(ROOT_DIR, LOG_FOLDER_NAME)
PIPELINE_DIR = os.path.join(ROOT_DIR, PIPELINE_FOLDER_NAME)
MODEL_DIR = os.path.join(ROOT_DIR, SAVED_MODELS_DIR_NAME)

HOUSING_DATA_KEY = "housing_data"
MEDIAN_HOUSING_VALUE_KEY = "median_house_value"

app = Flask(__name__)
pipeline=Pipeline()



@app.route('/artifact', defaults={'req_path': 'housing'})
@app.route('/artifact/<path:req_path>')
def render_artifact_dir(req_path):
    os.makedirs("housing", exist_ok=True)
    # Joining the base and the requested path
    print(f"req_path: {req_path}")
    abs_path = os.path.join(req_path)
    print(abs_path)
    # Return 404 if path doesn't exist
    if not os.path.exists(abs_path):
        return abort(404)

    # Check if path is a file and serve
    if os.path.isfile(abs_path):
        if ".html" in abs_path:
            with open(abs_path, "r",encoding="utf-8") as file:
                content = ''
                for line in file.readlines():
                    content = f"{content}{line}"
                return content
        return send_file(abs_path)

    # Show directory contents
    files = {os.path.join(abs_path, file): file for file in os.listdir(abs_path)}

    result = {
        "files": files,
        "parent_folder": os.path.dirname(abs_path),
        "parent_label": abs_path
    }
    return render_template('files.html', result=result)


@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        return render_template('index.html')
    except Exception as e:
        return str(e)


@app.route('/view_experiment_hist', methods=['GET', 'POST'])
def view_experiment_history():
    
    experiment_list = pipeline.get_experiment_history()
    context = {
        "experiment_list":[experiment.to_html(classes='table table-striped') for experiment in experiment_list]
    }
    return render_template('experiment_history.html',context=context)

@app.route('/train', methods=['GET', 'POST'])
def train():
    if not pipeline.experiment.running_status:
        pipeline.start()
    context = {
        "experiment": pipeline.get_experiment_status().to_html(classes='table table-striped')
    }
    return render_template('train.html',context=context)


@app.route('/predict', methods=['GET', 'POST'])
def predict():
    context = {
        HOUSING_DATA_KEY: None,
        MEDIAN_HOUSING_VALUE_KEY: None
    }

    if request.method == 'POST':
        longitude = float(request.form['longitude'])
        latitude = float(request.form['latitude'])
        housing_median_age = float(request.form['housing_median_age'])
        total_rooms = float(request.form['total_rooms'])
        total_bedrooms = float(request.form['total_bedrooms'])
        population = float(request.form['population'])
        households = float(request.form['households'])
        median_income = float(request.form['median_income'])
        ocean_proximity = request.form['ocean_proximity']

        housing_data = HousingData(longitude=longitude,
                                   latitude=latitude,
                                   housing_median_age=housing_median_age,
                                   total_rooms=total_rooms,
                                   total_bedrooms=total_bedrooms,
                                   population=population,
                                   households=households,
                                   median_income=median_income,
                                   ocean_proximity=ocean_proximity,
                                   )
        housing_df = housing_data.get_housing_input_data_frame()
        housing_predictor = HousingPredictor(model_dir=MODEL_DIR)
        median_housing_value = housing_predictor.predict(X=housing_df)
        context = {
            HOUSING_DATA_KEY: housing_data.get_housing_data_as_dict(),
            MEDIAN_HOUSING_VALUE_KEY: median_housing_value,
        }
        return render_template('predict.html', context=context)
    return render_template("predict.html", context=context)


@app.route('/saved_models', defaults={'req_path': 'saved_models'})
@app.route('/saved_models/<path:req_path>')
def saved_models_dir(req_path):
    os.makedirs("saved_models", exist_ok=True)
    # Joining the base and the requested path
    print(f"req_path: {req_path}")
    abs_path = os.path.join(req_path)
    print(abs_path)
    # Return 404 if path doesn't exist
    if not os.path.exists(abs_path):
        return abort(404)

    # Check if path is a file and serve
    if os.path.isfile(abs_path):
        return send_file(abs_path)

    # Show directory contents
    files = {os.path.join(abs_path, file): file for file in os.listdir(abs_path)}

    result = {
        "files": files,
        "parent_folder": os.path.dirname(abs_path),
        "parent_label": abs_path
    }
    return render_template('saved_models_files.html', result=result)


@app.route('/logs', defaults={'req_path': 'logs'})
@app.route('/logs/<path:req_path>')
def render_log_dir(req_path):
    os.makedirs("logs", exist_ok=True)
    # Joining the base and the requested path
    print(f"req_path: {req_path}")
    abs_path = os.path.join(req_path)
    print(abs_path)
    # Return 404 if path doesn't exist
    if not os.path.exists(abs_path):
        return abort(404)

    # Check if path is a file and serve
    if os.path.isfile(abs_path):
        return send_file(abs_path)

    # Show directory contents
    files = {os.path.join(abs_path, file): file for file in os.listdir(abs_path)}

    result = {
        "files": files,
        "parent_folder": os.path.dirname(abs_path),
        "parent_label": abs_path
    }
    return render_template('log_files.html', result=result)



if __name__=="__main__":
    app.run()
