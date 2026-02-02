import math
import torch
import torch.nn as nn

class MultiheadAttention(nn.Module):
    def __init__(self, d_model, num_heads):
        assert d_model % num_heads = 0
        self.d_model = d_model
