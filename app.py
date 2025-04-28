import pynecone as pc

class State(pc.State):
    missing_indices: list[str] = ["index1", "index2"]
    
    def refresh(self):
        # Add ES query here
        self.missing_indices = ["index3", "index4"]

def index():
    return pc.vstack(
        pc.heading("ES Sync", font_size="2em"),
        pc.button("Refresh", on_click=State.refresh),
        pc.list(
            pc.foreach(State.missing_indices, lambda idx: pc.list_item(idx)),
        )
    )

app = pc.App(state=State)
app.add_page(index)
app.compile()