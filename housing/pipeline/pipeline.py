from collections import namedtuple
from datetime import datetime
import uuid
from housing.config.configuration import Configuartion
from housing.logger import logging,get_log_file_name
from housing.exception import HousingException
from threading import Thread
from typing import List
from multiprocessing import Process
from housing.entity.artifact_entity import ModelPusherArtifact, DataIngestionArtifact,ModelEvaluationArtifact
from housing.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact, ModelTrainerArtifact
from housing.entity.config_entity import DataIngestionConfig,ModelEvaluationConfig
from housing.component.data_ingestion import DataIngestion
from housing.component.data_validation import DataValidation
from housing.component.data_transformation import DataTransformation
from housing.component.model_trainer import ModelTrainer
from housing.component.model_evaluation import ModelEvaluation
from housing.component.model_pusher import ModelPusher
import os, sys
from collections import namedtuple
from datetime import datetime
import pandas  as pd
from housing.constant import EXPERIMENT_DIR_NAME
Experiment = namedtuple("Experiment",["experiment_id","initialization_timestamp","log_file_name",
"running_status","start_time","stop_time","execution_time","message","experiment_file_path"])

class Pipeline(Thread):
    
    experiment:Experiment=Experiment(*([None]*9))

    def __new__(cls, *args,**kwargs):
        if Pipeline.experiment.running_status:
            raise Exception("Pipeline is already running")
        return super(Pipeline,cls).__new__(cls)

    def __init__(self, config: Configuartion = Configuartion()) -> None:
        try:
            super().__init__(daemon=False, name="pipeline")
            self.config = config
        except Exception as e:
            raise HousingException(e, sys) from e

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise HousingException(e, sys) from e

    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) \
            -> DataValidationArtifact:
        try:
            data_validation = DataValidation(data_validation_config=self.config.get_data_validation_config(),
                                             data_ingestion_artifact=data_ingestion_artifact
                                             )
            return data_validation.initiate_data_validation()
        except Exception as e:
            raise HousingException(e, sys) from e

    def start_data_transformation(self,
                                  data_ingestion_artifact: DataIngestionArtifact,
                                  data_validation_artifact: DataValidationArtifact
                                  ) -> DataTransformationArtifact:
        try:
            data_transformation = DataTransformation(
                data_transformation_config=self.config.get_data_transformation_config(),
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_artifact=data_validation_artifact
            )
            return data_transformation.initiate_data_transformation()
        except Exception as e:
            raise HousingException(e, sys)

    def start_model_trainer(self,data_transformation_artifact:DataTransformationArtifact)->ModelTrainerArtifact:
        try:
            model_trainer = ModelTrainer(model_trainer_config=self.config.get_model_trainer_config(),
            data_transformation_artifact=data_transformation_artifact
            )
            return model_trainer.initiate_model_trainer()
        except Exception as e:
            raise HousingException(e,sys) from e

    def start_model_evaluation(self, data_ingestion_artifact: DataIngestionArtifact,
            data_validation_artifact: DataValidationArtifact,
            model_trainer_artifact: ModelTrainerArtifact)->ModelEvaluationArtifact:
        try:
            model_eval = ModelEvaluation(
                model_evaluation_config=self.config.get_model_evaluation_config(),
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_artifact=data_validation_artifact,
                model_trainer_artifact=model_trainer_artifact)
            return model_eval.initiate_model_evaluation()
        except Exception as e:
            raise HousingException(e, sys) from e

    def start_model_pusher(self,model_eval_artifact: ModelEvaluationArtifact) -> ModelPusherArtifact:
        try:
            model_pusher = ModelPusher(
                model_pusher_config=self.config.get_model_pusher_config(),
                model_evaluation_artifact=model_eval_artifact
            )
            return model_pusher.initiate_model_pusher()
        except Exception as e:
            raise HousingException(e, sys) from e

    def run_pipeline(self):
        try:
            if Pipeline.experiment.running_status:
                logging.info("Pipeline is already running")
                return Pipeline.experiment
            # data ingestion
            logging.info("Pipeline starting.")

            experiment_id=str(uuid.uuid4())
            artifact_dir = os.path.join(self.config.training_pipeline_config.artifact_dir,EXPERIMENT_DIR_NAME)
            os.makedirs(artifact_dir,exist_ok=True)
            file_name = f"experiment-{experiment_id}.csv"
            experiment_file_path=os.path.join(artifact_dir,file_name)
            
            Pipeline.experiment = Experiment(experiment_id=experiment_id,
            initialization_timestamp=self.config.time_stamp,
            log_file_name=get_log_file_name(self.config.time_stamp),
            running_status=True,
            start_time=datetime.now(),
            stop_time=None,
            execution_time=None,
            experiment_file_path=experiment_file_path,
            message="Pipeline has been started."
            )
            logging.info(f"Pipeline experiment: {Pipeline.experiment}")

            self.save_experiment()
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_artifact=data_validation_artifact
            )
            model_trainer_artifact=self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)

            model_evaluation_artifact = self.start_model_evaluation(data_ingestion_artifact=data_ingestion_artifact,
                                                                    data_validation_artifact=data_validation_artifact,
                                                                    model_trainer_artifact=model_trainer_artifact)

            if model_evaluation_artifact.is_model_accepted:
                model_pusher_artifact = self.start_model_pusher(model_eval_artifact=model_evaluation_artifact)
                logging.info(f'Model pusher artifact: {model_pusher_artifact}')
            else:
                logging.info("Trained model rejected.")
            logging.info("Pipeline completed.")
            

            stop_time= datetime.now()
            Pipeline.experiment = Experiment(experiment_id=Pipeline.experiment.experiment_id,
            initialization_timestamp=self.config.time_stamp,
            log_file_name=get_log_file_name(self.config.time_stamp),
            running_status=True,
            start_time=Pipeline.experiment.start_time,
            stop_time=stop_time,
            execution_time=stop_time-Pipeline.experiment.start_time,
            message="Pipeline has been completed."
            )
            logging.info(f"Pipeline experiment: {Pipeline.experiment}")
            self.save()
        except Exception as e:
            raise HousingException(e, sys) from e

    def run(self):
        try:
            self.run_pipeline()
        except Exception as e:
            raise e


    def save_experiment(self):
        try:
            if Pipeline.experiment.experiment_id is not None:
                experiment = Pipeline.experiment
                experiment_report = pd.DataFrame(zip(experiment._fields,experiment))
                experiment_report.to_csv(experiment.experiment_file_path,mode="w",index=False,header=False)
            else:
                print("First start experiment")
        except Exception as e:
            raise HousingException(e,sys) from e    
    
    def get_experiment_history(self,limit=5)->List[pd.DataFrame]:
        try:
            experiment_dir=os.path.join(self.config.training_pipeline_config.artifact_dir,EXPERIMENT_DIR_NAME)

            if not os.path.exists(experiment_dir):
                return [pd.DataFrame()]

            experiment_files = os.listdir(experiment_dir)

            if len(experiment_files)==0:
                return [pd.DataFrame()]
            experiment_files.sort(reverse=True)
            experiment_dataframe_list:pd.DataFrame=[]
            
            for file_name in experiment_files[:limit]:
                experiment_file_path = os.path.join(experiment_dir,file_name)
                experiment_dataframe_list.append(pd.read_csv(experiment_file_path))
            return experiment_dataframe_list
        except Exception as e:
            raise HousingException(e,sys) from e

    def get_experiment_status(self,)->pd.DataFrame:
        try:
            if Pipeline.experiment.experiment_id is not None:
                return pd.read_csv(Pipeline.experiment.experiment_file_path,header=None)
            else:
                return pd.DataFrame()
                print("Experment is not yet started")
        except Exception as e:
            raise HousingException(e,sys)