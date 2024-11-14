import { useState, useEffect } from "preact/hooks";
import { API_URL } from "../routes/api.ts";

type Props = {
  batchId: string;
};

type Data = {
  value: number;
};

export default function Events({ batchId }: Props) {
  const [events, setEvents] = useState<Data[]>([]);

  // * Literally I didn't know how to do this, so I had to look at the solution surfing the web and AI
  // * I never used EventSource and nothing similar to StreamingResponse in FastAPI before
  // * I'm not sure if this is the best way, it works but is not too synchronized
  useEffect(() => {
    const eventSource = new EventSource(`${API_URL}/stream/${batchId}`);

    eventSource.addEventListener("message", (event) => {
      const data: Data = JSON.parse(event.data);
      setEvents((prevEvents) => [...prevEvents, data]);
    });

    return () => {
      eventSource.close();
    };
  }, [batchId]);

  return (
    <div>
      <ul>
        {events.map((event, i) => (
          <li key={i}>{event.value}</li>
        ))}
      </ul>
    </div>
  );
}
