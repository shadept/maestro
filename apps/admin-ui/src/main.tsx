/* @refresh reload */
import { render } from "solid-js/web";
import { App } from "./App.tsx";
import "./styles.css";

const root = document.getElementById("root");
if (root === null) throw new Error("missing #root element");

render(() => <App />, root);
