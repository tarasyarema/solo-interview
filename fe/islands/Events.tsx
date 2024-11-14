import { useState } from "https://esm.sh/v128/preact@10.22.0/hooks";

type Props = {
  batchId: string;
};

type Data = {
  value: number;
};

export default function Events(_: Props) {
  const [events, _setEvents] = useState<Data[]>([]);

  // Implement stuff here

  return (
    <div>
      <ul>
        {events.map((event, i) => <li key={i}>{event.value}</li>)}
      </ul>
    </div>
  );
}
