import net from "net";
import { tableFromArrays, tableToIPC } from "apache-arrow";

const values = tableFromArrays({
  tags: ["aa", "bb", "cc"],
  request: [1, 2, 3],
  error: [false, false, false],
});

const serialized = tableToIPC(values);

const main = async (path: string) => {
  const ipcClient = net
    .createConnection(path)
    .on("connect", () => console.log("connected"))
    .on("error", (err) => console.log(`error: ${err}`));

  ipcClient.write(serialized);
};

await main("/tmp/trails/example.socket");

export default {};
