# imports
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from omr import process_omr
from similarityhybrid import compute_similarity_hybrid

# constants
OPTIONS = ["123610", "Select folder"]

# global variables
selected_dir = ''


def on_option_selected(event):
    global selected_dir
    selected_option = dropdown_var.get()
    if selected_option == OPTIONS[-1]:
        selected_dir_aux = filedialog.askdirectory()
        if selected_dir_aux:
            selected_dir = selected_dir_aux
            directory_label.config(text=f"folder: {selected_dir}")
    else:
        directory_label.config(text="")  # clean label


def omr_process():
    selected_option = dropdown_var.get()
    root.destroy()
    if selected_option == OPTIONS[-1]:
        source = process_omr(selected_dir)
        compute_similarity_hybrid(source)
    else:
        process_omr(selected_option)
        compute_similarity_hybrid(selected_option)


def exit_program():
    root.destroy()


# main window
root = tk.Tk()
root.title("OMR processing")

# string var
dropdown_var = tk.StringVar(value=OPTIONS[0])

# dropdown
dropdown = ttk.Combobox(root, textvariable=dropdown_var, values=OPTIONS, state="readonly")
dropdown.bind("<<ComboboxSelected>>", on_option_selected)
dropdown.pack(pady=10)

# text label
directory_label = tk.Label(root, text="", wraplength=400, justify="left", fg="blue")
directory_label.pack(pady=10)

# buttons
continue_button = tk.Button(root, text="Process", command=omr_process)
continue_button.pack(side="left", padx=10, pady=10)

exit_button = tk.Button(root, text="Exit", command=exit_program)
exit_button.pack(side="right", padx=10, pady=10)

# gui loop
root.mainloop()
