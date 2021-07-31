import torch
from torch import nn


class Model:
    def __init__(
        self,
        model_dir: str,
        model_obj: nn.Module,
    ):
        self.__init_device()
        self.__load_model(model_dir, model_obj)
        self.model.eval()
        self.model.to(self.device)
            
    def __init_device(self):
        if torch.cuda.is_available():
            self.device = 'cuda'
        else:
            self.device = 'cpu'
        
    def __load_model(self, model_dir, model_obj):
        self.model = model_obj
        if self.device == 'cpu':
            self.model.load_state_dict(torch.load(model_dir, map_location=torch.device('cpu')))
        else:
            self.model.load_state_dict(torch.load(model_dir)) 