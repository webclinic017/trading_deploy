from math import erf, exp, log, pi, sqrt

from numba import jit


@jit(nopython=True)
def norm_pdf(x):
    return exp(-(x**2) / 2) / sqrt(2 * pi)


@jit(nopython=True)
def norm_cdf(x):
    return (1 + erf(x / sqrt(2))) / 2


@jit(nopython=True)
def bs_call(S, K, T, r, volatility):
    d1 = (log(S / K) + (r + (volatility**2) / 2) * T) / (volatility * sqrt(T))
    d2 = d1 - (volatility * sqrt(T))
    return S * norm_cdf(d1) - K * exp(-r * T) * norm_cdf(d2)


@jit(nopython=True)
def bs_put(S, K, T, r, volatility):
    d1 = (log(S / K) + (r + (volatility**2) / 2) * T) / (volatility * sqrt(T))
    d2 = d1 - (volatility * sqrt(T))
    return K * exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


@jit(nopython=True)
def find_call_greeks(
    target_value,
    S,
    K,
    T,
    r,
    lower_range=0,
    upper_range=15,
    max_iters=1000,
):
    for _ in range(max_iters):  # Don't iterate too much
        mid = (lower_range + upper_range) / 2
        price = bs_call(S, K, T, r, mid)  # BS Model Pricing

        if mid < 0.00001:
            mid = 0.00001
            break

        if round(price, 2) == target_value:
            break

        if price > target_value:
            upper_range = mid
        elif price < target_value:
            lower_range = mid

    volatility = mid
    d1 = (log(S / K) + (r + (volatility**2) / 2) * T) / (volatility * sqrt(T))
    d2 = d1 - (volatility * sqrt(T))

    delta = norm_cdf(d1)
    theta = (-S * norm_pdf(d1) * volatility / (2 * sqrt(T)) - r * K * exp(-r * T) * norm_cdf(d2)) / 365
    vega = S * norm_pdf(d1) * sqrt(T) / 100
    gamma = norm_pdf(d1) / (S * (volatility * sqrt(T)))
    return volatility, delta, theta, gamma, vega


@jit(nopython=True)
def find_put_greeks(
    target_value,
    S,
    K,
    T,
    r,
    lower_range=0,
    upper_range=15,
    max_iters=1000,
):
    for _ in range(max_iters):  # Don't iterate too much
        mid = (lower_range + upper_range) / 2
        price = bs_put(S, K, T, r, mid)  # BS Model Pricing

        if mid < 0.00001:
            mid = 0.00001
            break

        if round(price, 2) == target_value:
            break

        if price > target_value:
            upper_range = mid
        elif price < target_value:
            lower_range = mid
    volatility = mid

    d1 = (log(S / K) + (r + (volatility**2) / 2) * T) / (volatility * sqrt(T))
    d2 = d1 - (volatility * sqrt(T))

    delta = -norm_cdf(-d1)
    theta = (-S * norm_pdf(d1) * volatility / (2 * sqrt(T)) + r * K * exp(-r * T) * norm_cdf(-d2)) / 365
    vega = S * norm_pdf(d1) * sqrt(T) / 100
    gamma = norm_pdf(d1) / (S * (volatility * sqrt(T)))
    return volatility, delta, theta, gamma, vega


def find_greeks(target_value, S, K, T, r, o):
    if o in ["CE", 1]:
        return find_call_greeks(target_value, S, K, T, r)
    else:
        return find_put_greeks(target_value, S, K, T, r)
