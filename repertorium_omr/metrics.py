import torch

# -------------------------------------------- CTC DECODERS:

def ctc_greedy_decoder(y_pred, i2w):
    # Best path
    y_pred_decoded = torch.argmax(y_pred, dim=1)
    # Merge repeated elements
    y_pred_decoded = torch.unique_consecutive(y_pred_decoded, dim=0).tolist()
    # Convert to string; len(i2w) -> CTC-blank
    y_pred_decoded = [i2w[i] for i in y_pred_decoded if i != len(i2w)]
    return y_pred_decoded