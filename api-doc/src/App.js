import React, { useState, useEffect } from "react";
import SwaggerUI from "swagger-ui-react";
import "swagger-ui-react/swagger-ui.css";

function App() {
  const [yamlFile, setYamlFile] = useState();
  useEffect(() => {
    fetch("./custom_api.yaml", {
      method: "get",
      headers: new Headers({
        "content-type": "text/plain",
      }),
    })
      .then((res) => res.text())
      .then((result) => {
        setYamlFile(result);
      });
  }, []);

  return <SwaggerUI spec={yamlFile} />;
}

export default App;
