import { API_URL } from "../routes/api.ts";
import { Button } from "../components/Button.tsx";

type Props = {
  batchId: string;
};

export default function BatchDelete({ batchId }: Props) {
  const disabled = !batchId || batchId.length === 0;

  const handleDelete = async () => {
    try {
      const res = await fetch(`${API_URL}/stream/${batchId}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        throw new Error("Error deleting batch");
      }
    } catch (e: any) {
      console.error(e);
    }
  };

  return <>{!disabled && <Button onClick={handleDelete}>Stop</Button>}</>;
}
