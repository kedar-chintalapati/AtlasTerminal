/**
 * Polls the backend /api/v1/health endpoint and exposes
 * connectivity status for the status bar.
 */
import { useQuery } from "@tanstack/react-query";

interface HealthResponse {
  status: string;
  store: string;
  scheduler: string;
  version: string;
}

export function useBackendHealth() {
  const { data, isError, isFetching } = useQuery<HealthResponse>({
    queryKey: ["backend-health"],
    queryFn: async () => {
      const res = await fetch("/api/v1/health");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    },
    refetchInterval: 15_000,        // check every 15 s
    retry: 1,
    retryDelay: 2000,
    staleTime: 10_000,
  });

  return {
    online: !isError && !!data,
    checking: isFetching && !data,
    store: data?.store ?? "unknown",
    scheduler: data?.scheduler ?? "unknown",
    version: data?.version ?? "",
  };
}
