import net from "net";
import { tableFromArrays } from "apache-arrow";

const rainAmounts = Float32Array.from({ length: 5 }, () =>
  Number((Math.random() * 20).toFixed(1))
);

const rainDates = Array.from(
  { length: 5 },
  (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i)
);

const rainfall = tableFromArrays({
  precipitation: rainAmounts,
  date: rainDates,
});

const main = async (path: string) => {
  const ipcClient = net
    .createConnection(path)
    .on("connect", () => console.log("connected"))
    .on("error", (err) => console.log(`error: ${err}`));
};
